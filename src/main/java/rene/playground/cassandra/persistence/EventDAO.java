package rene.playground.cassandra.persistence;

import java.time.LocalDateTime;

import rene.playground.cassandra.persistence.model.Event;
import rx.Observable;

/**
 * @author Rene Loperena <rene@vuh.io>
 *
 */
public interface EventDAO {
	/**
	 * @param movie
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	Observable<Event> getMovieEventsByDateRange(String movie, LocalDateTime startDate, LocalDateTime endDate);
}
