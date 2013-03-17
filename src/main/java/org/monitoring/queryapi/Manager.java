package org.monitoring.queryapi;

import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.MongoOptions;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.util.Properties;
import org.apache.log4j.Logger;

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
     * to connect on localhost:27017 and db name test
     *
     */
    public Manager() {
        loadProperties();
        connect();
    }

    /**
     * Tries to connect Mongo server
     */
    private void connect() {
        try {
            MongoOptions options = new MongoOptions();
            options.connectTimeout = 100;
            m = new Mongo(host + ":" + port, options);
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
            File f = new File("config.properties");
            is = new FileInputStream(f);
        } catch (Exception ex) {
            is = null;
        }

        try {
            if (is == null) {
                // Try loading from classpath
                is = getClass().getResourceAsStream("config.properties");
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
    
    public Query createQuery(){
        if(col == null){
            throw new NullPointerException("Collection was not set");
        }
        return new Query(col);
    }
    
    /**
     * Return Mongo DB instance
     */
    public DB getDb() {
        return db;
    }

    /**
     * Return Mongo DB Collection
     */
    public DBCollection getCollection() {
        return col;
    }
    
    public Manager setCollection(String collectionName){
        col = db.getCollection(collectionName);        
        return this;
    }

    /**
     * Read JS function from file on specified path and execute it on serverside of Mongo
     */
    public void executeJSFromFile(String path) {
        StringBuilder sb = new StringBuilder();
        InputStream is = null;
        try {
            File f = new File(path);
            is = new FileInputStream(f);
        } catch (IOException ex) {
            System.err.println("no file");
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
        executeJS(sb.toString());
    }
    
    /**
     * Execute JS function on serverside of Mongo
     * @param cmd one JS function
     */
    public void executeJS(String cmd){
        db.doEval(cmd);
    }
    

}
