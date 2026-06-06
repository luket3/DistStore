package message;

import communication.Comm;

public class Reply extends Message {
    public Comm comm;
    public int reply_num;

    public Reply(Comm comm, int reply_num) {
        this.type = "Reply";
        this.comm = comm;
        this.reply_num = reply_num;
    }
    
}
