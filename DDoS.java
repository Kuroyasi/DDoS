public class CheckerDDOS {
    private static final int LIMIT_REQUEST = 100;

    public void check(String ip, ContainerRequestContext container) {

        int requestCount = getRecentRequestCount(ip); 

        if (requestCount >= LIMIT_REQUEST) {

container.abortWith(Response.status(Response.Status.TOO_MANY_REQUESTS)
                    .entity("Too many requests. Your IP has been temporarily blocked.")
                    .build());
        } else {
            incrementRequestCount(ip);
        }
    }
}


@Component
public class RedisRateLimitService {
    private final ReactiveRedisTemplate<String, String> redisTemplate;

    public Mono<Boolean> isAllowed(String key, int limit, int windowSeconds) {
        String redisKey = "rate_limit:" + key;
        long now = System.currentTimeMillis();
        long windowStart = now - (windowSeconds * 1000L);

        return redisTemplate.opsForZSet()
                .removeRangeByScore(redisKey, 0, windowStart)
                .then(redisTemplate.opsForZSet().count(redisKey, windowStart, now))
                .flatMap(count -> {
                    if (count < limit) {
                        return redisTemplate.opsForZSet()
                                .add(redisKey, UUID.randomUUID().toString(), now)
                                .then(redisTemplate.expire(redisKey, Duration.ofSeconds(windowSeconds)))
                                .thenReturn(true);
                    } else {
                        return Mono.just(false);
                    }
                });
    }
}

public class TrafficAnomalyDetector {
    private final double baselineRate;
    private final double thresholdMultiplier = 5.0;

    public boolean isUnderAttack(ServiceMetrics currentMetrics) {
        double currentRate = currentMetrics.getRequestRatePerSecond();
        double threshold = baselineRate * thresholdMultiplier;
        return currentRate > threshold; 
    }
}
