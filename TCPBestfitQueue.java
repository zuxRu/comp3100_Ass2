
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

public class TCPBestfitQueue {

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

    Job currentJob = new Job("null");
    HashMap<String, Job> allJobs = new HashMap<String, Job>();// maps jobID to the orriginal job so i can get it back on
                                                              // complete
    String numberofServers;
    String[] nums;
    String str2;
    ArrayList<Server> allServers = new ArrayList<>();
    while (!currentJob.jobType.equals("NONE")) {
      // 9: Send REDY
      sendMsg(out, "REDY");
      String str = "";
      str = recieveMsg(in);
      if (str.equals(".") || str.equals("")) {
        continue;
      }
      currentJob = new Job(str);
      if (currentJob.jobType.equals("JOBN")) {
        allJobs.put(currentJob.jobId, currentJob);
      }

      // put while here
      if (currentJob.jobType.equals("JOBN")) {
        /// start idea
        sendMsg(out, "GETS All");

        numberofServers = recieveMsg(in);
        nums = numberofServers.split(" ");
        System.out.println("O-K 1");
        sendMsg(out, "OK");
        str2 = "";
        allServers = new ArrayList<Server>();
        for (int i = 0; i < Integer.parseInt(nums[1]); i++) {// recieve all servers
          str2 = recieveMsg(in);
          Server tempServer = new Server(str2);
          allServers.add(tempServer);

        }

        Collections.sort(allServers, (j1, j2) -> {
          if (j1.cores > j2.cores) {
            return 0;
          } else {
            return 1;
          }
        });
        System.out.println("O-K 2");
        sendMsg(out, "OK");// testing
        recieveMsg(in);
        // end idea
        sendMsg(out, "ENQJ GQ");// enqj
        recieveMsg(in);// recieve ok

        sendMsg(out, "LSTQ GQ *");
        str2 = recieveMsg(in);
        System.out.println("O-K 3");

        sendMsg(out, "OK");

        int lengthOfQueue = Integer.parseInt(str2.split(" ")[1]);
        ArrayList<Job> jobsToQueue = new ArrayList<>();
        for (int i = 0; i < lengthOfQueue; i++) {// recieve all servers
          str2 = recieveMsg(in);
          jobsToQueue.add(new Job(allJobs.get(str2.split(" ")[0]), i));

        }

        Collections.sort(jobsToQueue, (j1, j2) -> {
          if (j1.estRuntime > j2.estRuntime) {
            return 0;
          } else {
            return 1;
          }
        });
        System.out.println("O-K 4");
        sendMsg(out, "OK");
        recieveMsg(in);

        if (lengthOfQueue > 1) {
          reset: for (int i = 0; i < lengthOfQueue; i++) { // if i find a server to scheudule to get a new server and
            // job list
            // and reset i back to zero and j to zero

            for (int j = 0; j < allServers.size(); j++) {

              if (allServers.get(j).cores >= jobsToQueue.get(i).core
                  && allServers.get(j).memory >= jobsToQueue.get(i).memory
                  && allServers.get(j).disk >= jobsToQueue.get(i).disk && allServers.get(j).currentJobs < 1) {

                String shudU = "SCHD " + jobsToQueue.get(i).jobId + " " + allServers.get(j).serverType + " "
                    + allServers.get(j).serverId;

                sendMsg(out, "DEQJ GQ " + jobsToQueue.get(i).queueLocation);
                recieveMsg(in);

                sendMsg(out, shudU);
                recieveMsg(in);
                // System.out.println("O-K 5");

                // sendMsg(out, "OK");// testing
                // recieveMsg(in);

                sendMsg(out, "GETS All");

                numberofServers = recieveMsg(in);
                nums = numberofServers.split(" ");
                System.out.println("O-K 6");

                sendMsg(out, "OK");
                str2 = "";
                allServers = new ArrayList<Server>();
                for (int z = 0; z < Integer.parseInt(nums[1]); z++) {// recieve all servers
                  str2 = recieveMsg(in);
                  Server tempServer = new Server(str2);
                  allServers.add(tempServer);

                }
                sendMsg(out, "OK");
                recieveMsg(in);
                Collections.sort(allServers, (j1, j2) -> {
                  if (j1.cores > j2.cores) {
                    return 0;
                  } else {
                    return 1;
                  }
                });
                // makes it this far and what am i doing
                // sendMsg(out, "ENQJ GQ");// enqj
                // recieveMsg(in);// recieve ok

                sendMsg(out, "LSTQ GQ *");
                System.out.println("prior");
                str2 = recieveMsg(in);
                System.out.println("latter");

                lengthOfQueue = Integer.parseInt(str2.split(" ")[1]);
                sendMsg(out, "OK");
                jobsToQueue = new ArrayList<>();
                for (int z = 0; z < lengthOfQueue; z++) {// recieve all servers
                  str2 = recieveMsg(in);
                  jobsToQueue.add(new Job(allJobs.get(str2.split(" ")[0]), z));

                }

                Collections.sort(jobsToQueue, (j1, j2) -> {
                  if (j1.estRuntime > j2.estRuntime) {
                    return 0;
                  } else {
                    return 1;
                  }

                });
                System.out.println("O-K 7");

                sendMsg(out, "OK");
                recieveMsg(in);
                break reset;

              }
            }
          }
        } else {

          boolean jobSheduled = false;
          System.out
              .println("Makes it and specs are " + currentJob.core + " " + currentJob.memory + " " + currentJob.disk);
          for (int i = 0; i < allServers.size(); i++) {
            if (!jobSheduled) {
              if (allServers.get(i).cores >= currentJob.core && allServers.get(i).memory >= currentJob.memory
                  && allServers.get(i).disk >= currentJob.disk && allServers.get(i).currentJobs < 1) {
                String shudU = "SCHD " + currentJob.jobId + " " + allServers.get(i).serverType + " "
                    + allServers.get(i).serverId;
                allServers.get(i).startJob(currentJob);
                sendMsg(out, "DEQJ GQ " + 0);
                recieveMsg(in);

                sendMsg(out, shudU);
                recieveMsg(in);
                jobSheduled = true;
                break;

              }
            }
          }
          // System.out.println("makes it to end of first for loop");
          // if (!jobSheduled) {
          // // sendMsg(out, "ENQJ GQ");// enqj
          // // recieveMsg(in);// recieve ok

          // }
        }
      }
      if (currentJob.jobType.equals("JCPL")) {
        continue;
        // // sendMsg(out, "ENQJ GQ");// enqj
        // // recieveMsg(in);// recieve ok

        // sendMsg(out, "LSTQ GQ *");
        // str2 = recieveMsg(in);
        // sendMsg(out, "OK");
        // int lengthOfQueue = Integer.parseInt(str2.split(" ")[1]);
        // ArrayList<Job> jobsToQueue = new ArrayList<>();
        // for (int i = 0; i < lengthOfQueue; i++) {// recieve all servers
        // str2 = recieveMsg(in);
        // jobsToQueue.add(new Job(allJobs.get(str2.split(" ")[0]), i));

        // }

        // Collections.sort(jobsToQueue, (j1, j2) -> {
        // if (j1.estRuntime > j2.estRuntime) {
        // return 0;
        // } else {
        // return 1;
        // }
        // });
        // System.out.println("O-K 8");

        // sendMsg(out, "OK");
        // recieveMsg(in);

        // recieveMsg(in);// twice the recieve the better

        // if (lengthOfQueue > 0) {
        // reset: for (int i = 0; i < lengthOfQueue; i++) { // if i find a server to
        // scheudule to get a new server and
        // // job list
        // // and reset i back to zero and j to zero

        // for (int j = 0; j < allServers.size(); j++) {

        // if (allServers.get(j).cores >= jobsToQueue.get(i).core
        // && allServers.get(j).memory >= jobsToQueue.get(i).memory
        // && allServers.get(j).disk >= jobsToQueue.get(i).disk &&
        // allServers.get(j).currentJobs < 1) {

        // String shudU = "SCHD " + jobsToQueue.get(i).jobId + " " +
        // allServers.get(j).serverType + " "
        // + allServers.get(j).serverId;

        // sendMsg(out, "DEQJ GQ " + jobsToQueue.get(i).queueLocation);
        // recieveMsg(in);

        // sendMsg(out, shudU);
        // recieveMsg(in);
        // // System.out.println("O-K 5");

        // // sendMsg(out, "OK");// testing
        // // recieveMsg(in);

        // sendMsg(out, "GETS All");

        // numberofServers = recieveMsg(in);
        // nums = numberofServers.split(" ");
        // System.out.println("O-K 6");

        // sendMsg(out, "OK");
        // str2 = "";
        // allServers = new ArrayList<Server>();
        // for (int z = 0; z < Integer.parseInt(nums[1]); z++) {// recieve all servers
        // str2 = recieveMsg(in);
        // Server tempServer = new Server(str2);
        // allServers.add(tempServer);

        // }
        // sendMsg(out, "OK");
        // recieveMsg(in);
        // Collections.sort(allServers, (j1, j2) -> {
        // if (j1.cores > j2.cores) {
        // return 0;
        // } else {
        // return 1;
        // }
        // });
        // // makes it this far and what am i doing
        // // sendMsg(out, "ENQJ GQ");// enqj
        // // recieveMsg(in);// recieve ok

        // sendMsg(out, "LSTQ GQ *");
        // System.out.println("prior");
        // str2 = recieveMsg(in);
        // System.out.println("latter");

        // lengthOfQueue = Integer.parseInt(str2.split(" ")[1]);
        // sendMsg(out, "OK");
        // jobsToQueue = new ArrayList<>();
        // for (int z = 0; z < lengthOfQueue; z++) {// recieve all servers
        // str2 = recieveMsg(in);
        // jobsToQueue.add(new Job(allJobs.get(str2.split(" ")[0]), z));

        // }

        // Collections.sort(jobsToQueue, (j1, j2) -> {
        // if (j1.estRuntime > j2.estRuntime) {
        // return 0;
        // } else {
        // return 1;
        // }

        // });
        // System.out.println("O-K 7");

        // sendMsg(out, "OK");
        // System.out.println("Yar she blows");
        // recieveMsg(in);
        // break reset;

        // }
        // }
        // }

        // }
      }
      if (currentJob.jobType.equals("CHKQ")) {
        // start
        sendMsg(out, "GETS All");

        numberofServers = recieveMsg(in);
        nums = numberofServers.split(" ");
        System.out.println("O-K 1");
        sendMsg(out, "OK");
        str2 = "";
        allServers = new ArrayList<Server>();
        for (int i = 0; i < Integer.parseInt(nums[1]); i++) {// recieve all servers
          str2 = recieveMsg(in);
          Server tempServer = new Server(str2);
          allServers.add(tempServer);

        }

        Collections.sort(allServers, (j1, j2) -> {
          if (j1.cores > j2.cores) {
            return 0;
          } else {
            return 1;
          }
        });
        System.out.println("O-K 2");
        sendMsg(out, "OK");// testing
        recieveMsg(in);
        // end
        sendMsg(out, "LSTQ GQ *");
        str2 = recieveMsg(in);
        System.out.println("O-K 3");

        sendMsg(out, "OK");

        int lengthOfQueue = Integer.parseInt(str2.split(" ")[1]);
        ArrayList<Job> jobsToQueue = new ArrayList<>();
        for (int i = 0; i < lengthOfQueue; i++) {// recieve all servers
          str2 = recieveMsg(in);
          jobsToQueue.add(new Job(allJobs.get(str2.split(" ")[0]), i));

        }

        Collections.sort(jobsToQueue, (j1, j2) -> {
          if (j1.estRuntime > j2.estRuntime) {
            return 0;
          } else {
            return 1;
          }
        });
        System.out.println("O-K 4");
        sendMsg(out, "OK");
        recieveMsg(in);

        if (lengthOfQueue >= 1) {
          reset: for (int i = 0; i < lengthOfQueue; i++) { // if i find a server to scheudule to get a new server and
            // job list
            // and reset i back to zero and j to zero

            for (int j = 0; j < allServers.size(); j++) {
              if (lengthOfQueue <= 0) {
                break;
              }
              if (allServers.get(j).cores >= jobsToQueue.get(i).core
                  && allServers.get(j).memory >= jobsToQueue.get(i).memory
                  && allServers.get(j).disk >= jobsToQueue.get(i).disk && allServers.get(j).currentJobs < 1) {

                String shudU = "SCHD " + jobsToQueue.get(i).jobId + " " + allServers.get(j).serverType + " "
                    + allServers.get(j).serverId;

                sendMsg(out, "DEQJ GQ " + jobsToQueue.get(i).queueLocation);
                recieveMsg(in);

                sendMsg(out, shudU);
                recieveMsg(in);
                // System.out.println("O-K 5");

                // sendMsg(out, "OK");// testing
                // recieveMsg(in);

                sendMsg(out, "GETS All");

                numberofServers = recieveMsg(in);
                nums = numberofServers.split(" ");
                System.out.println("O-K 6");

                sendMsg(out, "OK");
                str2 = "";
                allServers = new ArrayList<Server>();
                for (int z = 0; z < Integer.parseInt(nums[1]); z++) {// recieve all servers
                  str2 = recieveMsg(in);
                  Server tempServer = new Server(str2);
                  allServers.add(tempServer);

                }
                sendMsg(out, "OK");
                recieveMsg(in);
                Collections.sort(allServers, (j1, j2) -> {
                  if (j1.cores > j2.cores) {
                    return 0;
                  } else {
                    return 1;
                  }
                });
                // makes it this far and what am i doing
                // sendMsg(out, "ENQJ GQ");// enqj
                // recieveMsg(in);// recieve ok

                sendMsg(out, "LSTQ GQ *");
                str2 = recieveMsg(in);

                lengthOfQueue = Integer.parseInt(str2.split(" ")[1]);
                sendMsg(out, "OK");
                jobsToQueue = new ArrayList<>();
                for (int z = 0; z < lengthOfQueue; z++) {// recieve all servers
                  str2 = recieveMsg(in);
                  jobsToQueue.add(new Job(allJobs.get(str2.split(" ")[0]), z));

                }

                Collections.sort(jobsToQueue, (j1, j2) -> {
                  if (j1.estRuntime > j2.estRuntime) {
                    return 0;
                  } else {
                    return 1;
                  }

                });
                sendMsg(out, "OK");
                recieveMsg(in);
                if (lengthOfQueue > 0) {
                  break reset;
                }

              }
            }
          }
        }

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
