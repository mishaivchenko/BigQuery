package settings;

import org.kohsuke.args4j.Option;

public class Settings {
    @Option(name = "-dataSet",required = true, usage = "set dataSet")
    private String dataSet;
    @Option(name = "-project",required = true, usage = "set project")
    private String project;
    @Option(name="-count", required = true, usage = "set count")
    private int count;
    @Option(name="-prefix", required = true, usage = "set prefix")
    private String prefix;

    public String getDataSet() {
        return dataSet;
    }

    public String getProject() {
        return project;
    }

    public int getCount() {
        return count;
    }

    public String getPrefix() {
        return prefix;
    }

    @Override
    public String toString() {
        return "Settings{" +
                "dataSet='" + dataSet + '\'' +
                ", project='" + project + '\'' +
                ", count=" + count +
                ", prefix='" + prefix + '\'' +
                '}';
    }
}
