package message;

import cluster.Node;;

public class DictMsg extends Reply {
    public String action;
    public String key;
    public String value;

    public DictMsg(String action, String key, String value, int version) {
        super("DictMsg", version);
        this.action = action;
        this.key = key;
        this.value = value;

    }

    public DictMsg(String action, String key, String value, int version, Node client) {
        super("DictMsg", version, client);
        this.action = action;
        this.key = key;
        this.value = value;
    }
}
