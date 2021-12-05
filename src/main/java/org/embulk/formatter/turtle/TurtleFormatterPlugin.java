package org.embulk.formatter.turtle;

import com.google.common.base.Optional;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.FileOutput;
import org.embulk.spi.FormatterPlugin;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;

public class TurtleFormatterPlugin
        implements FormatterPlugin
{
    public interface PluginTask
            extends Task
    {
        // configuration option 1 (required integer)
        @Config("option1")
        public int getOption1();

        // configuration option 2 (optional string, null is not allowed)
        @Config("option2")
        @ConfigDefault("\"myvalue\"")
        public String getOption2();

        // configuration option 3 (optional string, null is allowed)
        @Config("option3")
        @ConfigDefault("null")
        public Optional<String> getOption3();
    }

    @Override
    public void transaction(ConfigSource config, Schema schema,
            FormatterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        control.run(task.dump());
    }

    @Override
    public PageOutput open(TaskSource taskSource, Schema schema,
            FileOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        // Write your code here :)
        throw new UnsupportedOperationException("TurtleFormatterPlugin.open method is not implemented yet");
    }
}
