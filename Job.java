import java.util.Arrays;

public class Job {
    public String jobType;
    public String submitTime;
    public String jobId;
    public String serverId;
    public int estRuntime;
    public int core;
    public int memory;
    public int disk;
    public int queueLocation;

    public Job(String entery) {
        String[] tempHold = entery.split(" ");
        jobType = tempHold[0];

        if (jobType.equals("JOBN")) {
            submitTime = tempHold[1];
            jobId = tempHold[2];
            estRuntime = Integer.parseInt(tempHold[3]);
            core = Integer.parseInt(tempHold[4]);
            memory = Integer.parseInt(tempHold[5]);
            disk = Integer.parseInt(tempHold[6]);
        }
        if (jobType.equals("JCPL")) {
            jobId = tempHold[2];
            serverId = tempHold[4];
        }

    }

    public Job(Job j, int pos) {// this is insane
        this.jobType = j.jobType;
        this.submitTime = j.submitTime;
        this.jobId = j.jobId;
        this.serverId = j.serverId;
        this.estRuntime = j.estRuntime;
        this.core = j.core;
        this.memory = j.memory;
        this.disk = j.disk;
        this.queueLocation = pos;

    }

}

// JOBN submitTime jobID estRuntime core memory disk
// JOBP submitTime jobID estRuntime core memory disk
// JCPL endTime jobID serverType serverID
// RESF serverType serverID timeOfFailure
// RESR serverType serverID timeOfRecovery
// CHKQ - indicate no new jobs, but some in the queue
// NONE