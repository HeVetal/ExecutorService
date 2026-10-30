import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

public class ParallelSum {

    static long sumRange(int[] numbers, int indexStart, int indexFinish) {
        long sum = 0;
        for (int i = indexStart; i < indexFinish; i++) {
            sum += numbers[i];
        }
        return sum;
    }

    static long countSumExecutor(int[] numbers, int threads) throws ExecutionException, InterruptedException {
        try (ExecutorService executorService = Executors.newFixedThreadPool(threads)) {
            int chunkSize = numbers.length / threads;
            List<Future<Long>> futureList = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                int start = i * chunkSize;
                int end = (i == threads - 1) ? numbers.length : start + chunkSize;
                futureList.add(executorService.submit(() -> sumRange(numbers, start, end)));
            }

            long sum = 0;

            for (Future<Long> future : futureList) {
                sum += future.get();
            }
            return sum;
        }
    }

    static long countSumCompletableFuture(int[] numbers, int threads) {
        try (ExecutorService executorService = Executors.newFixedThreadPool(threads)) {
            int piesSize = numbers.length / threads;
            List<CompletableFuture<Long>> futureList = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                int start = i * piesSize;
                int end = (i == threads - 1) ? numbers.length : start + piesSize;
                futureList.add(CompletableFuture.supplyAsync(() -> sumRange(numbers, start, end), executorService));
            }

            return CompletableFuture
                    .allOf(futureList.toArray(new CompletableFuture[0]))
                    .thenApply(v -> futureList.stream()
                            .mapToLong(CompletableFuture::join)
                            .sum())
                    .join();
        }
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        int size = 10_000_000;
        int cores = Runtime.getRuntime().availableProcessors();
        int[] numbers = new Random()
                .ints(size, 1, 101).toArray();

        System.out.println(sumRange(numbers, 0, numbers.length));

        System.out.println(countSumExecutor(numbers, 0));

        System.out.println(countSumCompletableFuture(numbers, cores));
    }
}

