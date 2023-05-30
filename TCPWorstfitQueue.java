
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

public class TCPWorstfitQueue {

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

  public static String recieveMsgN(BufferedReader in) {
    String msg = "";
    try {
      msg = in.readLine();
      // System.out.println("RCVD: " + msg);

    } catch (Exception e) {
      // TODO: handle exception
    }
    return msg;

  }

  public static void sendMsgN(DataOutputStream out, String msg) {

    try {
      String temp = msg + "\n";
      out.write(temp.getBytes());
      out.flush();
      // System.out.println("SENT: " + msg);

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

  public static ArrayList<Server> getAndSortServers(DataOutputStream out, BufferedReader in) {

    sendMsg(out, "GETS All");
    String numberofServers = recieveMsg(in);
    String[] nums = numberofServers.split(" ");
    sendMsg(out, "OK");
    String str2 = "";

    ArrayList<Server> allServers = new ArrayList<Server>();
    for (int i = 0; i < Integer.parseInt(nums[1]); i++) {// recieve all servers
      str2 = recieveMsg(in);
      Server tempServer = new Server(str2);
      allServers.add(tempServer);
    }
    sendMsg(out, "OK");
    recieveMsg(in);
    Collections.sort(allServers, (j1, j2) -> {
      if (j1.cores > j2.cores) {
        return 1;
      } else {
        return 0;
      }
    });

    return allServers;

  }

  public static ArrayList<Job> getAndSortJobs(DataOutputStream out, BufferedReader in, HashMap<String, Job> allJobs) {

    sendMsg(out, "LSTQ GQ *");
    String str2 = recieveMsg(in);
    sendMsg(out, "OK");

    int lengthOfQueue = Integer.parseInt(str2.split(" ")[1]);
    ArrayList<Job> jobsToQueue = new ArrayList<>();
    if (lengthOfQueue == 0) {
      recieveMsg(in);
    } else {
      for (int i = 0; i < lengthOfQueue; i++) {// recieve all servers
        str2 = recieveMsg(in);
        jobsToQueue.add(new Job(allJobs.get(str2.split(" ")[0]), i));

      }
      sendMsg(out, "OK");
      recieveMsg(in);

      Collections.sort(jobsToQueue, (j1, j2) -> {
        if (j1.estRuntime > j2.estRuntime) {
          return 0;
        } else {
          return 1;
        }
      });
    }
    return jobsToQueue;
  }

  public static void sheduleAJob(DataOutputStream out, BufferedReader in, ArrayList<Server> servers,
      ArrayList<Job> jobs, HashMap<String, Job> allJobs) {
    int jobNotAbleToSchedule = 0;
    start: while (jobs.size() > 0 - jobNotAbleToSchedule) {
      jobs = getAndSortJobs(out, in, allJobs);
      for (int i = 0; i < jobs.size(); i++) {
        servers = getAndSortServers(out, in);
        Job currentJob = jobs.get(i);
        for (int j = 0; j < servers.size(); j++) {
          Server currentServer = servers.get(j);
          if (currentServer.cores >= currentJob.core && currentServer.memory >= currentJob.memory
              && currentServer.disk >= currentJob.disk) {
            // dequeue job
            sendMsg(out, "DEQJ GQ " + i);
            recieveMsg(in);
            // schedule job
            sendMsg(out, "SCHD " + currentJob.jobId + " " + currentServer.serverType + " " + currentServer.serverId);
            recieveMsg(in);
            break start;
          }
        }
      }
      break;
    }

  }

  public static void scheduleJob(BufferedReader in, DataOutputStream out) {

    Job currentJob = new Job("null");
    HashMap<String, Job> allJobs = new HashMap<String, Job>();// maps jobID to the orriginal job so i can get it back on
                                                              // complete

    while (!currentJob.jobType.equals("NONE")) {
      // 9: Send REDY
      sendMsg(out, "REDY");
      String str = "";
      str = recieveMsg(in);
      currentJob = new Job(str);

      if (currentJob.jobType.equals("JOBN")) {
        allJobs.put(currentJob.jobId, currentJob);
        sendMsg(out, "ENQJ GQ");
        recieveMsg(in);
        sheduleAJob(out, in, getAndSortServers(out, in), getAndSortJobs(out, in, allJobs), allJobs);

      }

      if (currentJob.jobType.equals("JCPL")) {
        sheduleAJob(out, in, getAndSortServers(out, in), getAndSortJobs(out, in, allJobs), allJobs);
      }

      if (currentJob.jobType.equals("CHKQ")) {
        sheduleAJob(out, in, getAndSortServers(out, in), getAndSortJobs(out, in, allJobs), allJobs);
      }
    }
  }

  public static void main(String[] args) {
    try {
      Socket s = new Socket("localhost", 50000);
      DataOutputStream out = new DataOutputStream(s.getOutputStream());
      BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
      String username = System.getProperty("user.name");

      int currentLine = new Throwable().getStackTrace()[0].getLineNumber();
      System.out.println("The Current Line Number is " + currentLine);

      login(in, out, username);
      scheduleJob(in, out);
      exit(in, out, s);

    } catch (Exception e) {
      System.out.println(e);
    }
  }
}
