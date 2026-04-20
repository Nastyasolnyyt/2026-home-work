package company.vk.edu.distrib.compute.solntseva_nastya;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolntsevaKVService implements KVService {
    private static final Logger log = LoggerFactory.getLogger(SolntsevaKVService.class);

    private static final int STATUS_OK = 200;
    private static final int STATUS_CREATED = 201;
    private static final int STATUS_ACCEPTED = 202;
    private static final int STATUS_BAD_REQUEST = 400;
    private static final int STATUS_NOT_FOUND = 404;
    private static final int STATUS_NOT_ALLOWED = 405;
    private static final int STATUS_GATEWAY_TIMEOUT = 504;
    private static final int NO_BODY = -1;

    private final HttpServer server;
    private final Dao<byte[]> dao;
    private final String myUrl;
    private final Set<String> topology;
    private final HttpClient httpClient;
    private final SolnHashiStrategy strategy;
    private final SolnConsistentHashRouter consistentRouter;
    private final SolnRendezvousHashRouter rendezvousRouter;

    private record Response(
            int status,
            byte[] body
    ) {
    }

    public SolntsevaKVService(final int port, final Dao<byte[]> dao,
                              final Set<String> topology, final String myUrl,
                              final SolnHashiStrategy strategy) throws IOException {
        this.dao = dao;
        this.myUrl = myUrl;
        this.topology = topology;
        this.strategy = strategy;

        if (strategy == SolnHashiStrategy.CONSISTENT) {
            this.consistentRouter = new SolnConsistentHashRouter(topology);
            this.rendezvousRouter = null;
        } else {
            this.consistentRouter = null;
            this.rendezvousRouter = new SolnRendezvousHashRouter(topology);
        }

        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(1))
                .build();

        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/v0/status", this::handleStatus);
        server.createContext("/v0/entity", this::handleEntity);
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
        try {
            dao.close();
        } catch (IOException e) {
            log.error("Close error", e);
        }
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        try (exchange) {
            int status = "GET".equals(exchange.getRequestMethod()) ? STATUS_OK : STATUS_NOT_ALLOWED;
            exchange.sendResponseHeaders(status, NO_BODY);
        }
    }

    private void handleEntity(HttpExchange exchange) {
        final String query = exchange.getRequestURI().getQuery();
        final String id = extractParam(query, "id");
        if (id == null || id.isEmpty()) {
            sendResponse(exchange, STATUS_BAD_REQUEST, null);
            return;
        }

        int n = extractInt(query, "from", topology.size());
        int ack = extractInt(query, "ack", 1);

        if (ack > n || ack <= 0) {
            sendResponse(exchange, STATUS_BAD_REQUEST, null);
            return;
        }

        List<String> targetNodes = getTargetNodes(id, n);
        byte[] requestBody = getRequestBody(exchange);
        String method = exchange.getRequestMethod();

        List<CompletableFuture<Response>> futures = new ArrayList<>();
        for (String node : targetNodes) {
            if (myUrl.equals(node)) {
                futures.add(CompletableFuture.supplyAsync(() -> handleLocalInternal(id, method, requestBody)));
            } else {
                futures.add(proxyAsync(node, exchange, method, requestBody));
            }
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .whenComplete((v, e) -> {
                    List<Response> responses = futures.stream()
                            .map(f -> f.getNow(new Response(500, null)))
                            .filter(r -> isSuccessful(r.status, method))
                            .toList();

                    if (responses.size() >= ack) {
                        Response best = responses.stream()
                                .filter(r -> r.status == STATUS_OK || r.status == STATUS_CREATED
                                        || r.status == STATUS_ACCEPTED)
                                .findFirst()
                                .orElse(responses.get(0));
                        sendResponse(exchange, best.status, best.body);
                    } else {
                        sendResponse(exchange, STATUS_GATEWAY_TIMEOUT, null);
                    }
                });
    }

    private Response handleLocalInternal(String id, String method, byte[] body) {
        try {
            return switch (method) {
                case "GET" -> new Response(STATUS_OK, dao.get(id));
                case "PUT" -> {
                    dao.upsert(id, body);
                    yield new Response(STATUS_CREATED, null);
                }
                case "DELETE" -> {
                    dao.delete(id);
                    yield new Response(STATUS_ACCEPTED, null);
                }
                default -> new Response(STATUS_NOT_ALLOWED, null);
            };
        } catch (NoSuchElementException e) {
            return new Response(STATUS_NOT_FOUND, null);
        } catch (Exception e) {
            return new Response(500, null);
        }
    }

    private CompletableFuture<Response> proxyAsync(String node, HttpExchange exchange, String method, byte[] body) {
        URI uri = URI.create(node + exchange.getRequestURI().toString());
        HttpRequest request = HttpRequest.newBuilder(uri)
                .method(method, HttpRequest.BodyPublishers.ofByteArray(body))
                .timeout(Duration.ofMillis(500))
                .build();

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
                .thenApply(r -> new Response(r.statusCode(), r.body()))
                .exceptionally(e -> new Response(500, null));
    }

    private boolean isSuccessful(int status, String method) {
        if ("GET".equals(method)) {
            return status == STATUS_OK || status == STATUS_NOT_FOUND;
        }
        return status == STATUS_CREATED || status == STATUS_ACCEPTED;
    }

    private List<String> getTargetNodes(String id, int n) {
        if (strategy == SolnHashiStrategy.CONSISTENT) {
            return consistentRouter.getNodes(id, n);
        }
        return rendezvousRouter.getNodes(id, n);
    }

    private void sendResponse(HttpExchange exchange, int status, byte[] body) {
        try (exchange) {
            int length = (body == null || body.length == 0) ? NO_BODY : body.length;
            exchange.sendResponseHeaders(status, length);
            if (body != null && body.length > 0) {
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(body);
                }
            }
        } catch (IOException e) {
            log.error("Response error", e);
        }
    }

    private byte[] getRequestBody(HttpExchange exchange) {
        try {
            return exchange.getRequestBody().readAllBytes();
        } catch (IOException e) {
            return new byte[0];
        }
    }

    private String extractParam(String query, String name) {
        if (query == null) {
            return null;
        }
        return Arrays.stream(query.split("&"))
                .map(s -> s.split("=", 2))
                .filter(a -> a.length == 2 && a[0].equals(name))
                .map(a -> a[1]).findFirst().orElse(null);
    }

    private int extractInt(String query, String name, int def) {
        String val = extractParam(query, name);
        return val == null ? def : Integer.parseInt(val);
    }
}
