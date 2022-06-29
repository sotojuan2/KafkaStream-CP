package com.example.colour;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class RandmonName {
    public static void main(String[] args) {
        String[] peoples = {"Bob","Jill","Tom","Brandon"};
        List<String> names = Arrays.asList(peoples);
        Collections.shuffle(names);
        /*for (String name : names) {
            System.out.print(name + " ");
        }*/
        int index = new Random().nextInt(names.size());
        String anynames = names.get(index);
        System.out.println("Your random name is: " + anynames + " now!");
    }
}
