package aggregator;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import persistence.impl.BigQueryRepositoryImpl;
import settings.Settings;

import java.io.IOException;

public class Main {
    public static void main(String[] args) {
        Settings settings = new Settings();
        CmdLineParser cmdLineParser = new CmdLineParser(settings);
        try {
            cmdLineParser.parseArgument(args);
            System.out.println("settings " + settings);
            BigQueryRepositoryImpl bigQueryService = new BigQueryRepositoryImpl(
                    settings.getProject(),
                    settings.getDataSet(),
                    settings.getPrefix()
            );
            bigQueryService.addTables(settings.getCount());
        } catch (CmdLineException | IOException e) {
            cmdLineParser.printUsage(System.out);
        }


    }

}
