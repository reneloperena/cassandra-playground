package rene.playground.cassandra.resources;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import rene.playground.cassandra.persistence.EventDAO;
import rene.playground.cassandra.persistence.EventDAOImpl;
import rene.playground.cassandra.persistence.model.Event;
import rene.playground.cassandra.resources.dto.EventInfo;
import rx.Observable;

/**
 * Implements the {@link MovieInformationResource}
 * 
 * @author Rene Loperena <rene@vuh.io>
 */
public class MovieInformationResourceImpl implements MovieInformationResource{
	
	private EventDAO eventDao = new EventDAOImpl();
	
	private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
	/* (non-Javadoc)
	 * @see rene.playground.cassandra.resources.MovieInformationResource#getMovieInformationBetweenDates(java.lang.String, java.util.Date, java.util.Date)
	 */
	public Response getMovieInformationBetweenDates(String movie, String startDate, String endDate){
		
		if(movie == null || startDate == null || endDate == null) return Response.status(Status.BAD_REQUEST).build();
		
		LocalDateTime startLocalDate = LocalDateTime.parse(startDate, formatter);
		LocalDateTime endLocalDate = LocalDateTime.parse(endDate, formatter);
		
		Observable<Event> events = eventDao.getMovieEventsByDateRange(movie, startLocalDate, endLocalDate);
		List<EventInfo> infoList = events.groupBy(event -> event.getEvent()).concatMap((group) -> group.reduce((event1, event2)->{
			event1.setCount(event2.getCount()+event1.getCount());
			return event1;
			}
		)).map(event->{
			EventInfo info = new EventInfo();
			info.setName(event.getEvent());
			info.setPartner(event.getPartner());
			info.setMovie(event.getMovie());
			info.setCount(event.getCount());
			info.setStartDate(startDate);
			info.setEndDate(endDate);
			return info;
		}).toList().toBlocking().first();
		// Happy Path
		return Response.ok().entity(infoList).build();
	
	}
}
