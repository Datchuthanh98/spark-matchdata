package Sv;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.Scanner;

public class C{
    public static void main(String[] args){
        Scanner input=new Scanner(System.in);
        try{
            Socket sock=new Socket("localhost",1211);
            System.out.println("[+] Connected.");
            //DataOutputStream data=new DataOutputStream(sock.getOutputStream());




            BufferedReader data = new BufferedReader(new InputStreamReader(sock.getInputStream()));
            String str_data="";
            while(!str_data.equals("Done")){
                str_data=data.readLine();
                //str_data=data.readUTF();
                System.out.println("=> "+str_data);
            }
            data.close();
            //data.close();
            sock.close();
        }
        catch(Exception e){System.out.println(e);}
    }
}