package logger;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.xml.XmlConfigurationFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import static util.FileUtils.SEP;

/* TODO adapt logging with LogSetup
import java.io.IOException;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

/**
 * Represents the initialization for the server logging with Log4J.
 */
public class LogSetup {

//    private static Logger logger = LogManager.getRootLogger();
    private String logdir;

    /**
     * Initializes the logging for the echo server. Logs are appended to the
     * console output and written into a separated server log file at a given
     * destination.
     *
     * @param logdir the destination (i.e. directory + filename) for the
     * 		persistent logging information.
     * @throws IOException if the log destination could not be found.
     */
    public LogSetup(String logdir, Level logLevel) throws IOException {
        this.logdir = logdir;
        initialize(logLevel);
    }

    private void initialize(Level logLevel) throws IOException {
        ConfigurationFactory factory =  XmlConfigurationFactory.getInstance();
        ConfigurationSource configurationSource = new ConfigurationSource(new FileInputStream(new File(System.getProperty("log4j.configurationFile"))));
        LoggerContext context = new LoggerContext("CloudDBLoggerContext");
        Configuration configuration = factory.getConfiguration(context, configurationSource);
        ConsoleAppender appender = ConsoleAppender.createDefaultAppenderForLayout(PatternLayout.createDefaultLayout());
        configuration.addAppender(appender);
        LoggerConfig loggerConfig = new LoggerConfig("com",Level.FATAL,false);
        loggerConfig.addAppender(appender,null,null);
        configuration.addLogger("com", loggerConfig);
        context.start(configuration);
//        Logger logger = context.getLogger("com");

//        PatternLayout layout = new PatternLayout( "%d{ISO8601} %-5p [%t] %c: %m%n" );
//        FileAppender fileAppender = new FileAppender( layout, logdir, true );

//        ConsoleAppender consoleAppender = new ConsoleAppender(layout);
//        logger.addAppender(consoleAppender);

//        logger.addAppender(fileAppender);
        // ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF:
//        loggerConfig.setLevel();
    }

}
