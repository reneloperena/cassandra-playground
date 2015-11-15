package rene.playground.cassandra;

import java.time.format.DateTimeFormatter;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

@Configuration
@PropertySource("classpath:persistence.properties")
public class ApplicationConfiguration {

	private Cluster cluster;
	private Session session;

	@Value("${table.name}")
	private String tableName;
	
	@Value("${keyspace}")
	private String keyspace;
	
	@Value("${cassandra.env.var}")
	private String cassandraEnvVariable;
	
	private void createClusterAndSession(){
		String ip = System.getenv().get(cassandraEnvVariable);
		if(ip == null) ip = "127.0.0.1";
		
		cluster = Cluster.builder().addContactPoint(ip).build();
		session = cluster.connect(keyspace);	
	}
	
	@Bean
	public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
		return new PropertySourcesPlaceholderConfigurer();
	}
	
	@Bean
	public DateTimeFormatter getDateTimeFormatter(){
		return  DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
	}

	@Bean
	public Cluster getCluster() {
		if(cluster == null) createClusterAndSession();
		return cluster;
	}

	@Bean
	public Session getSession() {
		if(session == null) createClusterAndSession();
		return session;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	public void setCassandraEnvVariable(String cassandraEnvVariable) {
		this.cassandraEnvVariable = cassandraEnvVariable;
	}
	
	

}
