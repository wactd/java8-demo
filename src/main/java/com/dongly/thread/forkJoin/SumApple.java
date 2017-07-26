package com.dongly.thread.forkJoin;


import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Fork/Join框架计算不同苹果类别
 */
public class SumApple {

    private final List<Apple> apples;

    {
        apples = new ArrayList<>();
        Apple apple;
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0; i < 10; i++) {
            apple = new Apple();
            apple.setId(i + 1);
            apple.setColour(getColour(i));
            apple.setWeight(random.nextInt(100, 500));
            apples.add(apple);
        }
    }


    public static void main(String[] args) {
        SumApple apple = new SumApple();
        List<Apple> appleApples = apple.getApples();

        Predicate<Apple> predicate = new Predicate<Apple>() {
            @Override
            public boolean test(Apple apple) {
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return apple.getId() % 2 == 1;
            }
        };

        Instant startTime = Instant.now();

        ForkJoinSumApple sumApple = new ForkJoinSumApple(appleApples, 0, appleApples.size(), predicate);
        ForkJoinPool pool = new ForkJoinPool(5);
        List<Apple> arrayList = pool.invoke(sumApple);
        System.out.println(arrayList.size()); // 200 0000 1100 0000 700 0000 900 0000  1800 0000

        Instant endTime = Instant.now();
        System.out.println(Duration.between(startTime, endTime).getNano() + "--"); // 20000000
    }

    static class ForkJoinSumApple extends RecursiveTask<List<Apple>> {

        private Integer start;
        private Integer end;
        // 临界值
        private Integer threshhold = 2;
        private Predicate<Apple> predicate;

        public ForkJoinSumApple(List<Apple> apples, Integer start, Integer end, Predicate<Apple> predicate) {
            super.setRawResult(apples);
            this.start = start;
            this.end = end;
            this.predicate = predicate;
        }

        @Override
        protected List<Apple> compute() {
            List<Apple> apples = super.getRawResult();
            Integer length = end - start;

            if (length <= threshhold) {
                List<Apple> appleList = new ArrayList<>();
                for (int i = start; i < end; i++) {
                    Apple apple = apples.get(i);
                    if (predicate.test(apple))
                        appleList.add(apple);
                }
                return appleList;
            } else {
                Integer middle = (start + end) / 2;
                ForkJoinSumApple left = new ForkJoinSumApple(apples, start, middle, predicate);
                left.fork();
                ForkJoinSumApple right = new ForkJoinSumApple(apples, middle, end, predicate);
                right.fork();
                List<Apple> join = left.join();
                join.addAll(right.join());
                return join;
            }
        }
    }

    /**
     * 获取苹果集
     *
     * @return 苹果集
     */
    public List<Apple> getApples() {
        return apples;
    }

    /**
     * 随机获取颜色
     *
     * @param index 下标
     * @return 颜色
     */
    public Colour getColour(Integer index) {
        Colour colour;
        switch (index % 3) {
            case 1:
                colour = Colour.BLACK;
                break;
            case 2:
                colour = Colour.BLUE;
                break;
            default:
                colour = Colour.YELLOW;
        }
        return colour;
    }

    public static class Apple {
        private Integer id;
        private Integer weight;
        private Colour colour;

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public Integer getWeight() {
            return weight;
        }

        public void setWeight(Integer weight) {
            this.weight = weight;
        }

        public Colour getColour() {
            return colour;
        }

        public void setColour(Colour colour) {
            this.colour = colour;
        }
    }

    public enum Colour {
        BLUE, YELLOW, BLACK
    }
}
