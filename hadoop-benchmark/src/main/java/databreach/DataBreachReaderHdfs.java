package databreach;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import utils.SearchFiles;

import java.util.ArrayList;
import java.util.Scanner;

public class DataBreachReaderHdfs {

    public static void main(String[] args) throws Exception {
        Configuration conf2 = new Configuration();

        Job job2 = Job.getInstance(conf2, "DataBreachFinder");
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        job2.setJarByClass(DataBreachHdfs.class);
        job2.waitForCompletion(true);

        do {
            System.out.println("If you want co check email, press 1. If you want to check password, press 2.");
            Scanner input = new Scanner(System.in);
            int number = input.nextInt();
            if (number == 1) {
                System.out.println("Your email: ");
                Scanner input2 = new Scanner(System.in);
                String email = input2.nextLine();
                System.out.println(args[0]);
                ArrayList<String> directories = SearchFiles.search(args[0], email);
                if (directories.isEmpty())
                    System.out.println("Your email is safe! It is not present in any of leaked user data.");
                else {
                    System.out.println("Your email is present in " + directories.size() + " data breaches: ");
                    for (String name : directories)
                        System.out.println(name);
                }
            } else {
                System.out.println("Your password: ");
                String password = input.nextLine();
                ArrayList<String> directories = SearchFiles.search(args[1], password);
                if (directories.isEmpty())
                    System.out.println("Your password is safe! It is not present in any of leaked user data.");
                else {
                    System.out.println("Your password is present in " + directories.size() + " data breaches: ");
                    for (String name : directories)
                        System.out.println(name);
                }

            }

        } while (true);
    }

}