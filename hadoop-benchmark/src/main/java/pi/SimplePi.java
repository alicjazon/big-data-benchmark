package pi;

import java.util.Scanner;

public class SimplePi {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        System.out.print("n = ");
        int n = sc.nextInt();
        long start = System.nanoTime();

        double x, y;
        int count = 0;
        for (int i = 1; i <= n; i++) {
            x = Math.random() * 2 - 1;
            y = Math.random() * 2 - 1;
            if (x * x + y * y <= 1) count++;
        }
        double p = 4. * count / (n - 1);
        System.out.println("Pi is: " + p);

        long end = System.nanoTime();
        long elapsedTime = end - start;
        System.out.println("Elapsed time " + elapsedTime + " ns");
    }
}
