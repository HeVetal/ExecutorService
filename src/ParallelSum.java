import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

public class ParallelSum {

    static long countSum(int[] numbers, int indexStart, int indexFinish) {
        long sum = 0;
        for (int i = indexStart; i < indexFinish; i++) {
            sum += numbers[i];
        }
        return sum;
    }

    static long countSumExecutor(int[] numbers, int cores) throws ExecutionException, InterruptedException {
        try (ExecutorService executorService = Executors.newFixedThreadPool(cores)) {
            int piesSize = numbers.length / cores;
            List<Future<Long>> futureList = new ArrayList<>();
            for (int i = 0; i < cores; i++) {
                int start = i * piesSize;
                int end = (i == cores - 1) ? numbers.length : start + piesSize;
                futureList.add(executorService.submit(() -> countSum(numbers, start, end)));
            }

            long sum = 0;

            for (Future<Long> longFuture : futureList) {
                sum += longFuture.get();
            }
            return sum;
        }
    }

    static long countSumCompletableFuture(int[] numbers, int cores) {
        try (ExecutorService executorService = Executors.newFixedThreadPool(cores)) {
            int piesSize = numbers.length / cores;
            List<CompletableFuture<Long>> futureList = new ArrayList<>();
            for (int i = 0; i < cores; i++) {
                int start = i * piesSize;
                int end = (i == cores - 1) ? numbers.length : start + piesSize;
                futureList.add(CompletableFuture.supplyAsync(() -> countSum(numbers, start, end), executorService));
            }

            return CompletableFuture
                    .allOf(futureList.toArray(new CompletableFuture[0]))
                    .thenApply(v -> futureList.stream()
                            .mapToLong(CompletableFuture::join)
                            .sum())
                    .join();
        }
    }

    static void main(String[] args) throws ExecutionException, InterruptedException {
        int size = 10_000_000;
        int cores;
        int[] numbers = new Random()
                .ints(size, 1, 101).toArray();

        cores = Runtime.getRuntime().availableProcessors();

        System.out.println(countSum(numbers, 0, numbers.length));

        System.out.println(countSumExecutor(numbers, cores));

        System.out.println(countSumCompletableFuture(numbers, cores));
    }
}

