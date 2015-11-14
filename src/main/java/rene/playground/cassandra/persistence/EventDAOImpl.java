package rene.playground.cassandra.persistence;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.util.List;

import javax.inject.Inject;

import org.apache.log4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.Lists;

import rene.playground.cassandra.persistence.model.Event;
import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;

/**
 * Implements the {@link EventDAO} interface using a Cassandra Database
 * 
 * @author Rene Loperena <rene@vuh.io>
 *
 */
public class EventDAOImpl implements EventDAO {
	
	private String keyspace = "analytics";

	private String table = "events_by_hour";

	private Cluster cluster = Cluster.builder().addContactPoint(System.getenv().get("CASSANDRA_IP")).build();

	private Session session = cluster.connect(keyspace);

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * rene.playground.cassandra.persistence.EventDAO#getMovieEventsByDateRange(
	 * java.lang.String, java.time.LocalDateTime, java.time.LocalDateTime)
	 */
	@Override
	public Observable<Event> getMovieEventsByDateRange(String movie, LocalDateTime startDateTime,
			LocalDateTime endDateTime) {

		// Creates the list of statements to be sent to the Cassandra cluster
		List<Statement> statements = createGetMovieEventsByDateRangeStatements(movie, startDateTime, endDateTime);

		// Queries all the statements to the database and receives an Observable
		// of ResultSets
		Observable<ResultSet> resultSets = queryAllStatements(statements);

		Observable<Event> events = resultSets
				// Will concatenate and map all ResultSets to a Row Observable
				.concatMap((resultSet) -> Observable.from(resultSet))
				// Will map each one of the rows to an Event object
				.map((row) -> {
					Event event = new Event();
					event.setEvent(row.getString("event"));
					event.setPartner(row.getString("partner"));
					event.setMovie(row.getString("movie"));
					event.setSource(row.getString("source"));
					event.setDateTime(row.getTimestamp("date_time"));
					event.setCount(row.getInt("count"));
					return event;
				});
		return events;
	}

	/**
	 * Given that we are using month-size bucket partitioning in Cassandra to
	 * avoid rows from getting too big, it is necessary to get the value of the
	 * bucket(s) involved in the query, and send each one of the CQL statements
	 * independently. The bucket value is determined by Year/Month. This
	 * function will take the Statement Information and generate all the
	 * different statements involved.
	 * 
	 * @param movie
	 * @param startDateTime
	 * @param endDateTime
	 * @return
	 */
	private List<Statement> createGetMovieEventsByDateRangeStatements(String movie, LocalDateTime startDateTime,
			LocalDateTime endDateTime) {
		/*
		 * Obtains LocalDate objects to obtain period (this will be used to
		 * obtain all the different buckets
		 */
		LocalDate startDate = LocalDate.of(startDateTime.getYear(), startDateTime.getMonth(),
				startDateTime.getDayOfMonth());
		LocalDate endDate = LocalDate.of(endDateTime.getYear(), endDateTime.getMonth(), endDateTime.getDayOfMonth());
		// Gets the range of months between both dates
		Period period = Period.between(startDate, endDate);
		int months = (period.getMonths() + 1)+(period.getYears()*12);
		// Creates an array list with expected size to avoid resizing
		List<Statement> statements = Lists.newArrayListWithExpectedSize(months);
		// For each one of the buckets, will create a statement
		for (int i = 0; i < months; i++) {
			// Date used to parse bucket, adds i months.
			LocalDate currentDate = startDate.plusMonths(i);
			// Creates statement
			Statement statement = new QueryBuilder(cluster).select().all().from(keyspace, table)
					.where(QueryBuilder.eq("movie", movie))
					.and(QueryBuilder.eq("month", currentDate.getYear() + "/" + currentDate.getMonthValue()))
					.and(QueryBuilder.gte("date_time", startDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)))
					.and(QueryBuilder.lte("date_time", endDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)));
			// Adds statement to the list
			statements.add(statement);
		}
		return statements;
	}

	/**
	 * Will use {@link #sendQueries(List)} and turn the response into a
	 * {@link ResultSet} JavaRx Observable
	 * 
	 * @param statements
	 * @return
	 */
	private Observable<ResultSet> queryAllStatements(List<Statement> statements) {
		// Gets the List of Futures
		List<ResultSetFuture> futures = sendQueries(statements);
		// Creates a scheduler object to be used (RxJava IO Scheduler)
		Scheduler scheduler = Schedulers.io();
		// Transforms the List of Futures to a List of ResultSet Observables
		// with the Scheduler
		List<Observable<ResultSet>> observables = Lists.transform(futures,
				(ResultSetFuture future) -> Observable.from(future, scheduler));
		// Merges all the Observable List to a ResultSet Observable
		return Observable.merge(observables);
	}

	/**
	 * Will execute asynchronously a list of {@link Statements} and return a
	 * list of futures {@link ResultSetFuture}.
	 * 
	 * @param statements
	 *            CQL statements
	 * @return List of {@link ResultSetFuture}
	 */
	private List<ResultSetFuture> sendQueries(List<Statement> statements) {
		// Creates an ArrayList with an expected size to avoid resizing
		List<ResultSetFuture> futures = Lists.newArrayListWithExpectedSize(statements.size());
		/*
		 * For each one of the statements, execute the statement asynchronously
		 * and add the returned Future to a list.
		 */
		for (Statement query : statements)
			futures.add(session.executeAsync(query.toString()));
		return futures;
	}
}
