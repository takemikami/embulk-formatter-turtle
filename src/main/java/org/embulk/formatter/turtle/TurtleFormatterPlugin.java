package org.embulk.formatter.turtle;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.jena.graph.*;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.*;
import org.apache.jena.tdb2.TDB2Factory;
import org.embulk.config.Config;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.*;
import org.embulk.spi.PageOutput;
import org.embulk.spi.util.FileOutputOutputStream;
import org.embulk.spi.util.FileOutputOutputStream.CloseMode;
import org.embulk.spi.type.BooleanType;
import org.embulk.spi.type.DoubleType;
import org.embulk.spi.type.JsonType;
import org.embulk.spi.type.LongType;
import org.embulk.spi.type.StringType;
import org.embulk.spi.type.TimestampType;

public class TurtleFormatterPlugin
        implements FormatterPlugin
{
    public interface PluginTask
            extends Task
    {
        // subject column name
        @Config("subject_column")
        public String getSubjectColumn();

        // columns: name and predicate
        @Config("columns")
        public List<Map<String, String>> getColumns();

        // base uri
        @Config("base")
        public String getBase();

        @ConfigInject
        public BufferAllocator getBufferAllocator();
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

        final FileOutputOutputStream stream = new FileOutputOutputStream(output, task.getBufferAllocator(), CloseMode.CLOSE);
        stream.nextFile();

        Optional<Column> subjectColumn = schema.getColumns().stream().filter(c -> c.getName().equals(task.getSubjectColumn())).findFirst();
        Map<String, String> colMap = task.getColumns().stream().collect(Collectors.toMap(c -> c.get("name"), c->c.get("predicate")));
        if (!subjectColumn.isPresent()) {
            throw new RuntimeException("subject column not found.");
        }
        Column subjectCol = subjectColumn.get();

        return new PageOutput() {
            private final PageReader pageReader = new PageReader(schema);
            private final List<Triple> triples = new LinkedList<>();

            private String getValueAsString(PageReader pageReader, Column col)
            {
                String rtn = null;
                if (col.getType() instanceof BooleanType) {
                    rtn = String.valueOf(pageReader.getBoolean(col));
                }
                else if (col.getType() instanceof LongType) {
                    rtn = String.valueOf(pageReader.getLong(col));
                }
                else if (col.getType() instanceof DoubleType) {
                    rtn = String.valueOf(pageReader.getDouble(col));
                }
                else if (col.getType() instanceof StringType) {
                    rtn = pageReader.getString(col);
                }
                else if (col.getType() instanceof TimestampType) {
                    rtn = String.valueOf(pageReader.getTimestamp(col));
                }
                else if (col.getType() instanceof JsonType) {
                    rtn = String.valueOf(pageReader.getJson(col));
                }
                return rtn;
            }

            @Override
            public void add(Page page)
            {
                pageReader.setPage(page);
                while (pageReader.nextRecord()) {
                    String subject = getValueAsString(pageReader, subjectCol);
                    Node subjectNode = NodeFactory.createURI(task.getBase() + subject);

                    for (Column col : schema.getColumns()) {
                        if (col == subjectCol) {
                            continue;
                        }
                        Node predNode = NodeFactory.createURI(colMap.get(col.getName()));
                        String object = getValueAsString(pageReader, col);
                        if (object == null) {
                            continue;
                        }
                        Node objectNode = NodeFactory.createLiteral(object);
                        triples.add(Triple.create(subjectNode, predNode, objectNode));
                    }
                }
            }

            @Override
            public void finish()
            {
                if (triples.size() == 0) {
                    stream.finish();
                    return;
                }

                Dataset dataset = TDB2Factory.createDataset();
                dataset.begin();
                Model model = dataset.getDefaultModel();
                for (Triple triple : triples) {
                    model.getGraph().add(triple);
                }
                RDFWriterBuilder builder = RDFWriter.create();
                builder.base(task.getBase())
                    .lang(Lang.TURTLE)
                    .source(model)
                    .output(stream);
                dataset.abort();
                dataset.end();
                stream.finish();
            }

            @Override
            public void close()
            {
                stream.close();
            }
        };
    }
}
