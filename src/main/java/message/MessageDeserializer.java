package message;

import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonDeserializationContext;
import java.lang.reflect.Type;

/**
 * Custom deserializer for Message objects that determines the concrete
 * message type based on the "type" field and delegates to the appropriate
 * subclass deserializer.
 */
public class MessageDeserializer implements JsonDeserializer<Message> {

    @Override
    public Message deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext ctx)
            throws JsonParseException {

        JsonObject obj = json.getAsJsonObject();
        String type = obj.get("type").getAsString();

        return switch (type) {
            case "AppendEntries" -> ctx.deserialize(json, AppendEntries.class);
            case "AppendEntriesReply" -> ctx.deserialize(json, AppendEntriesReply.class);
            case "DictMsg" -> ctx.deserialize(json, DictMsg.class);
            case "NodeMsg" -> ctx.deserialize(json, NodeMsg.class);
            case "Reply" -> ctx.deserialize(json, Reply.class);
            case "RequestVote" -> ctx.deserialize(json, RequestVote.class);
            case "RequestVoteReply" -> ctx.deserialize(json, RequestVoteReply.class);
            case "RaftConfig" -> ctx.deserialize(json, RaftConfig.class);
            case "SplitRaftConfig" -> ctx.deserialize(json, SplitRaftConfig.class);
            case "Config" -> ctx.deserialize(json, Config.class);
            case "Update" -> ctx.deserialize(json, Update.class);
            case "SplitShard" -> ctx.deserialize(json, SplitShard.class);
            case "Ack" -> ctx.deserialize(json, Ack.class);
            case "Response" -> ctx.deserialize(json, Response.class);
            default -> throw new JsonParseException("Unknown message type: " + type);
        };
    }
}