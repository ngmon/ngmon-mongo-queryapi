package org.monitoring.queryapi;

import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoOptions;
import com.mongodb.WriteConcern;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import org.apache.log4j.Logger;
import org.monitoring.queryapi.preaggregation.Preaggregate;
import org.monitoring.queryapi.preaggregation.PreaggregateMongoMRI;

/**
 *
 * @author Michal Dubravcik
 */
public class Manager {

    public static String CACHE;
    public static String CACHE_FLAGS;
    private Mongo m;
    private DB db;
    private DBCollection col;
    private String host;
    private int port;
    private String dbName;
    private static org.apache.log4j.Logger log =
            Logger.getLogger(Manager.class);
    
    /* default collection names for cache */
    private final String DEFAULT_CACHE = "cache";
    private final String DEFAULT_CACHE_FLAGS = "cache.flags";

    /**
     * Manager connects on Mongo server
     * @param host address
     * @param port numeric port
     * @param dbName name of database
     */
    public Manager(String host, int port, String dbName) {
        this.host = host;
        this.port = port;
        this.dbName = dbName;
        connect();
        CACHE = DEFAULT_CACHE;
        CACHE_FLAGS = DEFAULT_CACHE_FLAGS;
    }

    /**
     * Manager connects on Mongo server specified in file config.properties if no file found, tries
     * to connect on "localhost:27017" and DB "test"
     *
     */
    public Manager() {
        loadProperties();
        connect();
        executeJSSaveFromDefaultFile();
    }

    /**
     * Tries to connect Mongo server
     */
    private void connect() {
        try {
            MongoClientOptions options = MongoClientOptions.builder()
                    .connectTimeout(1000)
                    .writeConcern(WriteConcern.SAFE)
                    .build();            
            m = new MongoClient(host + ":" + port, options);
            db = m.getDB(dbName);
            db.collectionExists("test");
        } catch (UnknownHostException ex) {
             System.err.println("Connection failed: " + ex);
            throw new RuntimeException("Can not connect Mongo server");
        } catch (NullPointerException ex) {
            
            throw new RuntimeException("Can not connect Mongo server");
        } catch (Exception ex) {
            
            throw new RuntimeException("Can not connect Mongo server");
        }
    }

    /**
     * Read configuration from config.properties file
     */
    private void loadProperties() {
        Properties props = new Properties();
        InputStream is = null;

        // First try loading from the current directory
        try {
            File f = new File("src/main/resources/config.properties");
            is = new FileInputStream(f);
        } catch (Exception ex) {
            is = null;
        }

        try {
            if (is == null) {
                // Try loading from classpath
                is = getClass().getResourceAsStream("src/main/resources/config.properties");
            }

            props.load(is);

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        host = props.getProperty("mongo.host", "localhost");
        port = new Integer(props.getProperty("mongo.port", "27017"));
        dbName = props.getProperty("mongo.dbname", "test");

        CACHE = props.getProperty("mongo.cache", DEFAULT_CACHE);
        CACHE_FLAGS = props.getProperty("mongo.cache", DEFAULT_CACHE_FLAGS);
    }

    /**
     * Create query instance used for constructing complex db queries that will be executed on
     * specified collection
     *
     * @param col collection in db
     * @return new query with collection set
     */
    public Query createQueryOnCollection(String collectionName) {
        setCollection(collectionName);
        return new Query(col);
    }
    
    /**
     * Create query instance used for constructing complex db queries that will be executed on
     * default collection set in Manager
     *
     * @return new query with collection set
     */
    public Query createQuery(){
        if(col == null){
            throw new NullPointerException("Collection was not set");
        }
        return new Query(col);
    }
    
    /**
     * Create Preaggregation instance used for saving events and actualizing aggregation 
     * statistics online.
     * Incremental map reduce implementation is used as it is most prefered.
     *
     * @param col collection for saving events
     * @return preaggregation with collection set
     */
    public Preaggregate createPreaggregate(String collectionName){
        setCollection(collectionName);
        return new PreaggregateMongoMRI(col);
    }
    
    /**
     * Return Mongo DB instance which is Manager connected with
     */
    public DB getDb() {
        return db;
    }

    /**
     * Return Mongo DB Collection which is set as default in Manager
     */
    public DBCollection getCollection() {
        return col;
    }
    
    /**
     * Set default collection which can be later used for creating Query or Preaggregation instances
     * @param collectionName
     * @return Manager
     */
    public Manager setCollection(String collectionName){
        col = db.getCollection(collectionName);        
        return this;
    }

    /**
     * Read JS function from file on specified path and execute it on serverside of Mongo
     */
    public void executeJSFromFile(String path) {
        String cmd = readFile(path);
        executeJS(cmd);
    }
    
    private void executeJSSaveFromDefaultFile() {
        String cmd = readFile("src/main/resources/js_command.js");
        executeJS("db.system.js.save(" + cmd + ");");
    }
    
    public static String readFile(String path) {
        StringBuilder sb = new StringBuilder();
        InputStream is = null;
        try {
            File f = new File(path);
            is = new FileInputStream(f);
        } catch (IOException ex) {
            System.err.println("no file");
            throw new RuntimeException("no file find on path: " + path);
        }
        if (is == null) {
            is = Manager.class.getResourceAsStream(path);
        }
        InputStreamReader irs = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(irs);
        String line;
        try {
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
        } catch (IOException ex) {
            System.err.println("no file");
        }
        return sb.toString();
    }
    
    
    /**
     * Execute JS function on serverside of Mongo
     * @param cmd only one JS command (function)
     */
    public void executeJS(String cmd){
        CommandResult r = db.doEval(cmd);
    }
    
}
