public class Server {
    // 0 1 2 3 4 5 6
    // serverType serverID state curStartTime core memory disk #wJobs #rJobs
    // [#failures totalFailtime mttf mttr madf lastStartTime]
    public int cores;
    public int memory;
    public int disk;
    public String serverId;
    public String state;
    public String serverType;
    public int curStartTime;
    public int currentJobs;

    Server(String allInfo) {

        String[] infoAllServers = allInfo.split(" ");

        serverType = infoAllServers[0];
        serverId = infoAllServers[1];
        state = infoAllServers[2];
        curStartTime = Integer.parseInt(infoAllServers[3]);
        cores = Integer.parseInt(infoAllServers[4]);
        memory = Integer.parseInt(infoAllServers[5]);
        disk = Integer.parseInt(infoAllServers[6]);
        currentJobs = Integer.parseInt(infoAllServers[7]) + Integer.parseInt(infoAllServers[8]);

    }

    public void startJob(Job job) {
        cores -= job.core;
        memory -= job.memory;
        disk -= job.disk;
        currentJobs++;
    }

    public void endJob(Job job) {
        cores += job.core;
        memory += job.memory;
        disk += job.disk;
        currentJobs--;
    }

    public int getCores() {
        return cores;
    }

    public int getMemory() {
        return memory;
    }

    public int getDisk() {
        return disk;
    }

    public int getSartTime() {
        return curStartTime;

    }

    public String getServerId() {
        return serverId;
    }

    public String getState() {
        return state;
    }

    public String getServerType() {
        return serverType;
    }
}
