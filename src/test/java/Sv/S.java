package Sv;

import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

public class S {

    public static void main(String[] args){
        Scanner input=new Scanner(System.in);
        try{
            ServerSocket sock=new ServerSocket(1211);
            System.out.println("[+] Server Established!");
            Socket conn=sock.accept();//establishes connection
            System.out.println("[+] Client Connected.");
            //DataInputStream data = new DataInputStream(conn.getInputStream()));
            PrintWriter sender = new PrintWriter( new PrintWriter(conn.getOutputStream()));
            String d="";
            while(!d.equals("Done")){
                System.out.print(">> ");
                d=input.nextLine();
                sender.print(d+"\n");
                sender.flush();
                //data.writeUTF(d);
                //data.flush();
            }
            sock.close();
        }
        catch(Exception e){System.out.println(e);}
    }

}