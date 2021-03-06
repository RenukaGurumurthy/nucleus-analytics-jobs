package org.gooru.nucleus.consumer.sync.jobs.commands;

import java.util.HashMap;
import java.util.Map;

import org.gooru.nucleus.consumer.sync.jobs.constants.CommandConstants;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum CommandProcessorBuilder {

  DEFAULT("default") {
    private final Logger LOGGER = LoggerFactory.getLogger(CommandProcessorBuilder.class);

    @Override
    public void build(JSONObject event) {
      LOGGER.error("invalid command..");
    }
  },
  ITEM_DELETE(CommandConstants.ITEM_DELETE) {
    @Override
    public void build(JSONObject event) {
      CommandProcessorFactory.ItemDelete(event);
    }
  },
  ITEM_COPY(CommandConstants.ITEM_COPY) {
    @Override
    public void build(JSONObject event) {
      CommandProcessorFactory.ItemCopy(event);
    }
  },
  ITEM_MOVE(CommandConstants.ITEM_MOVE) {
    @Override
    public void build(JSONObject event) {
      CommandProcessorFactory.ItemMove(event);
    }
  },
  ITEM_CREATE(CommandConstants.ITEM_CREATE) {
    @Override
    public void build(JSONObject event) {
      CommandProcessorFactory.ItemCreate(event);
    }
  },
  ITEM_UPDATE(CommandConstants.ITEM_UPDATE) {
    @Override
    public void build(JSONObject event) {
      CommandProcessorFactory.ItemUpdate(event);
    }
  },
  CLASS_JOIN(CommandConstants.CLASS_JOIN) {
    @Override
    public void build(JSONObject context) {
      CommandProcessorFactory.ClassJoin(context);
    }
  },
  COLLABORATORS_UPDATE(CommandConstants.COLLABORATORS_UPDATE) {
	    @Override
	    public void build(JSONObject context) {
	      CommandProcessorFactory.CollaboratorsUpdate(context);
	    }
	  },
  CLASS_STUDENT_REMOVE(CommandConstants.CLASS_STUDENT_REMOVE) {
    @Override
    public void build(JSONObject context) {
      CommandProcessorFactory.ClassStudentRemove(context);
    }
    },
    
  USER_SIGN_IN(CommandConstants.USER_SIGN_IN) {
	    @Override
	    public void build(JSONObject context) {
	      CommandProcessorFactory.UserSignIn(context);
	    }
  },  
  USER_SIGN_OUT(CommandConstants.USER_SIGN_OUT) {
	    @Override
	    public void build(JSONObject context) {
	      CommandProcessorFactory.UserSignOut(context);
	    }
  };
    

  private String name;

  CommandProcessorBuilder(String name) {
    this.name = name;
  }

  public String getName() {
    return this.name;
  }

  private static final Map<String, CommandProcessorBuilder> LOOKUP = new HashMap<>();
  static {
    for (CommandProcessorBuilder builder : values()) {
      LOOKUP.put(builder.getName(), builder);
    }
  }

  public static CommandProcessorBuilder lookupBuilder(String name) {
    CommandProcessorBuilder builder = LOOKUP.get(name);
    if (builder == null) {
      return DEFAULT;
    }
    return builder;
  }

  public abstract void build(JSONObject context);
}
