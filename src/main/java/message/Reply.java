package message;

import communication.Comm;

public class Reply extends Message {
    public Comm comm;
    public int reply_num;

    public Reply(Comm comm, int reply_num, int version) {
        super("Reply", version);
        this.comm = comm;
        this.reply_num = reply_num;
    }

    public Reply(Comm comm, int version) {
        super("Reply", version);
        this.comm = comm;
        this.reply_num = -1;
    }
    
}
