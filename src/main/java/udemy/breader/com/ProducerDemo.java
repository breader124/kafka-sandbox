package udemy.breader.com;

import java.util.Random;
import java.util.stream.IntStream;

public class ProducerDemo {
    public static void main(String[] args) {
        Random engine = new Random();
        IntStream stream = IntStream.generate(engine::nextInt);
        stream.limit(10).forEach(System.out::println);
    }
}
