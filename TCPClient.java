
import java.io.IOException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;

import java.io.InputStreamReader;
import java.io.BufferedReader;

public class TCPClient {

  public static String recieveMsg(BufferedReader in) {
    String msg = "";
    try {
      msg = in.readLine();
      System.out.println("RCVD: " + msg);

    } catch (Exception e) {
      // TODO: handle exception
    }
    return msg;

  }

  public static void sendMsg(DataOutputStream out, String msg) {

    try {
      String temp = msg + "\n";
      out.write(temp.getBytes());
      out.flush();
      System.out.println("SENT: " + msg);

    } catch (Exception e) {
      // TODO: handle exception
    }

  }

  public static void login(BufferedReader in, DataOutputStream out, String username) {
    try {
      sendMsg(out, "HELO");
      recieveMsg(in);

      sendMsg(out, "AUTH " + username);
      recieveMsg(in);
    } catch (Exception e) {
      System.out.println("There was an error");
    }

  }

  public static void exit(BufferedReader in, DataOutputStream out, Socket s) {
    try {
      sendMsg(out, "QUIT");
      recieveMsg(in);
      in.close();
      out.close();
      s.close();

    } catch (Exception e) {
      // TODO: handle exception
    }

  }

  public static void scheduleJob(BufferedReader in, DataOutputStream out) {
    int count = 0;
    int modVar = 1;
    Job currentJob = new Job("null");
    // 9: Send REDY
    sendMsg(out, "REDY");

    String str = "";

    str = recieveMsg(in);
    currentJob = new Job(str);

    sendMsg(out, "GETS All");

    String numberofServers = recieveMsg(in);
    String[] nums = numberofServers.split(" ");

    sendMsg(out, "OK");
    String str2 = "";
    ArrayList<Server> allServers = new ArrayList<Server>();
    for (int i = 0; i < Integer.parseInt(nums[1]); i++) {// recieve all servers
      str2 = recieveMsg(in);
      System.out.println("server input is " + str2);
      Server tempServer = new Server(str2);
      allServers.add(tempServer);

    }

    // locate the maximum server array
    Server maxmServer = allServers.get(0);
    for (int i = 0; i < allServers.size(); i++) {
      if (allServers.get(i).cores > maxmServer.cores) {
        System.out
            .println("server new max " + allServers.get(i).serverType + "has cores " + allServers.get(i).cores);
        System.out.println("server old max " + maxmServer.serverType + "has cores " + maxmServer.cores);

        maxmServer = allServers.get(i);
      }

    }
    ArrayList<Server> useableServers = new ArrayList<Server>();

    for (int i = 0; i < allServers.size(); i++) {// get a list of all the servers that mare max first and have the
                                                 // same
                                                 // name
      if (allServers.get(i).serverType.equals(maxmServer.serverType)) {
        useableServers.add(allServers.get(i));
      }
    }

    modVar = useableServers.size();

    sendMsg(out, "OK");
    recieveMsg(in);

    while (!currentJob.jobType.equals("NONE")) {
      // 9: Send REDY

      // put while here
      if (currentJob.jobType.equals("JOBN")) {
        String shudU = "SCHD " + currentJob.jobId + " " + useableServers.get(count).serverType + " "
            + useableServers.get(count).serverId;
        sendMsg(out, shudU);
        recieveMsg(in);
        sendMsg(out, "OK");
        recieveMsg(in);
        recieveMsg(in);// trying this

        count++;
        count = count % modVar;
      }

      sendMsg(out, "REDY");

      str = "";

      str = recieveMsg(in);
      currentJob = new Job(str);

    }
  }

  public static void main(String[] args) {
    try {
      Socket s = new Socket("localhost", 50000);
      DataOutputStream out = new DataOutputStream(s.getOutputStream());
      BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
      String username = System.getProperty("user.name");

      login(in, out, username);
      scheduleJob(in, out);
      exit(in, out, s);

    } catch (Exception e) {
      System.out.println(e);
    }
  }
}
