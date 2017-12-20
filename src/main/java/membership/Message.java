package membership;

import java.io.Serializable;

public class Message<T> implements Serializable {

    String type;
    T content;

    public Message(String type, T content) {
        this.type = type;
        this.content = content;
    }
}
