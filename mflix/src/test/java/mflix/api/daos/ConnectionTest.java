package mflix.api.daos;

import com.mongodb.client.*;
import com.mongodb.client.FindIterable;
import mflix.config.MongoDBConfiguration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.bson.Document;

@SpringBootTest(classes = {MongoDBConfiguration.class})
@EnableConfigurationProperties
@EnableAutoConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
public class ConnectionTest extends TicketTest {

  private MovieDao dao;

  private String mongoUri;
  @Autowired MongoClient mongoClient;

  @Value("${spring.mongodb.database}")
  String databaseName;

  @Before
  public void setup() throws IOException {
    mongoUri = getProperty("spring.mongodb.uri");
    this.dao = new MovieDao(mongoClient, databaseName);
  }

  @Test
  public void testMoviesCount() {
    long expected =   23530;
    Assert.assertEquals("Check your connection string", expected, dao.getMoviesCount());
  }

  @Test
  public void testConnectionFindsDatabase() {

    MongoClient mc = MongoClients.create(mongoUri);
    boolean found = false;
    for (String dbname : mc.listDatabaseNames()) {
      if (databaseName.equals(dbname)) {
        found = true;
        break;
      }
    }
    Assert.assertTrue(
        "We can connect to MongoDB, but couldn't find expected database. Check the restore step",
        found);
  }

  @Test
  public void testConnectionFindsCollections() {

    MongoClient mc = MongoClients.create(mongoUri);
    // needs to find at least these collections
    List<String> collectionNames = Arrays.asList("comments", "movies", "sessions", "users");

    int found = 0;
    for (String colName : mc.getDatabase(databaseName).listCollectionNames()) {

      if (collectionNames.contains(colName)) {
        found++;
      }
    }

    Assert.assertEquals(
        "Could not find all expected collections. Check your restore step",
        found,
        collectionNames.size());
  }

  @Test
  public void testFindThirtyFirstMovie() {
    MongoDatabase database = mongoClient.getDatabase("sample_mflix");
    MongoCollection<Document> collection = database.getCollection("movies");
    AggregateIterable<Document> result = collection.aggregate(Arrays.asList(new Document("$match",
                    new Document("year", 1991L)),
            new Document("$sort",
                    new Document("title", 1L)),
            new Document("$skip", 30L),
            new Document("$project",
                    new Document("title", 1L))));
    Assert.assertEquals(
            "Returned wrong movie name",
            "Cape Fear",
            result.first().get("title"));
  }
}
