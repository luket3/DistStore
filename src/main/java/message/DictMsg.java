package message;

public class DictMsg extends Message {
    public String action;
    public String key;
    public String value;
    public int reply_num;

    public DictMsg(String action, String key, String value) {
        this.type = "DictMsg";
        this.action = action;
        this.key = key;
        this.value = value;
        this.reply_num = -1;
    }
}
