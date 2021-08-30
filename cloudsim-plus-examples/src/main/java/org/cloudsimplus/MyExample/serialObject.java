package org.cloudsimplus.MyExample;

import java.io.*;
import java.util.Set;

public class serialObject {

    public serialObject() {
    }

    public void serializableObject(Set<Long> set,String path) throws IOException {
        java.io.File file = new java.io.File(path);
        OutputStream os = new FileOutputStream(file);
        ObjectOutputStream oos = new ObjectOutputStream(os);
        oos.writeObject(set);
        oos.close();
        os.close();
        System.out.println("cloudlets id序列化完成了！");
    }

    public Set<Long> reverseSerializableObject(String path) throws IOException, ClassNotFoundException {
        java.io.File file = new java.io.File(path);
        InputStream input = new FileInputStream(file);
        ObjectInputStream ois = new ObjectInputStream(input);
        Set<Long> set = (Set<Long>)ois.readObject();
        ois.close();
        input.close();
        System.out.println("cloudlets id反序列化完成了！");
        return set;
    }

    public boolean checkObjectExist(String path){
        java.io.File file = new java.io.File(path);
        return file.exists();
    }

}
