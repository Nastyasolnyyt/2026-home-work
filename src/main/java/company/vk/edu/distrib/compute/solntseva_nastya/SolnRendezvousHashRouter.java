package company.vk.edu.distrib.compute.solntseva_nastya;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

public final class SolnRendezvousHashRouter {
    private final Collection<String> endpoints;

    public SolnRendezvousHashRouter(Collection<String> endpoints) {
        this.endpoints = endpoints;
    }

    // Возвращает n приоритетных узлов для ключа
    public List<String> getNodes(String key, int n) {
        return endpoints.stream()
                .sorted(Comparator.comparingInt((String node) -> hash(key + node)).reversed())
                .limit(n)
                .toList();
    }

    private static int hash(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] bytes = md.digest(input.getBytes(StandardCharsets.UTF_8));
            return ((bytes[0] & 0xFF) << 24) | ((bytes[1] & 0xFF) << 16) 
                 | ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 not found", e);
        }
    }
}
