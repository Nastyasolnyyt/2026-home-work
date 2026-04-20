package company.vk.edu.distrib.compute.solntseva_nastya;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public final class SolnConsistentHashRouter {
    private static final int VIRTUAL_NODES = 150;
    private final SortedMap<Integer, String> ring = new TreeMap<>();

    public SolnConsistentHashRouter(Iterable<String> endpoints) {
        for (String endpoint : endpoints) {
            for (int vn = 0; vn < VIRTUAL_NODES; vn++) {
                ring.put(hash(endpoint + "#" + vn), endpoint);
            }
        }
    }

    public List<String> getNodes(String key, int n) {
        if (ring.isEmpty()) {
            return List.of();
        }
        
        int hash = hash(key);
        List<String> result = new ArrayList<>();
        SortedMap<Integer, String> tailMap = ring.tailMap(hash);
        
        Iterator<String> it = tailMap.values().iterator();
        long distinctNodesCount = ring.values().stream().distinct().count();

        while (result.size() < n && result.size() < distinctNodesCount) {
            if (!it.hasNext()) {
                it = ring.values().iterator();
            }
            String node = it.next();
            if (!result.contains(node)) {
                result.add(node);
            }
        }
        return result;
    }

    private static int hash(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] bytes = md.digest(input.getBytes(StandardCharsets.UTF_8));
            return ((bytes[0] & 0xFF) << 24) | ((bytes[1] & 0xFF) << 16)
                 | ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF);
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException(ex);
        }
    }
}
