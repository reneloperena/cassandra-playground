package rene.playground.cassandra.resources;

import java.util.Date;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

/**
 * @author Rene Loperena <rene@vuh.io>
 *
 */
@Path("/v1/")
@Produces({ "application/json" })
public interface MovieInformationResource {
	/**
	 * @param movie
	 * @param startDate
	 * @param endDate
	 * @return
	 */
    @GET
    @Path("movie")
	Response getMovieInformationBetweenDates(
			@QueryParam(value="name") String name, 
			@QueryParam(value="startDate") String startDate, 
			@QueryParam(value="endDate") String endDate);
}
