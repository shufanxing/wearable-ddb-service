package com.shufan;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;
import com.shufan.util.Database;

@Path("/")
public class WebService {
    private Logger logger = Logger.getLogger(WebService.class);

    private Database db = new Database();    
	
    @Path("status")
    @GET
    @Produces(MediaType.TEXT_HTML)
    public String getStatus() {
        return "Server Status is OK...";
    }

    @POST
    @Path("{userID}/{day}/{timeInterval}/{stepCount}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response postStepCount(@PathParam("userID") Integer userID, @PathParam("day") Integer day, @PathParam("timeInterval") Integer timeInterval, @PathParam("stepCount") Integer stepCount) {
        return db.insertItem(userID, day, timeInterval, stepCount);
    }

    @GET
    @Path("current/{userID}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getCurrent(@PathParam("userID") Integer userID) {
        return db.queryItem(userID, 0, 0, 0, 23);

    }

    @GET
    @Path("single/{userID}/{day}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getSingle(@PathParam("userID") Integer userID, @PathParam("day") Integer day) {

        return db.queryItem(userID, day, 0, day, 23);
    }

    @GET
    @Path("range/{userID}/{startDay}/{numDays}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getRange(@PathParam("userID") Integer userID, @PathParam("startDay") Integer startDay, @PathParam("numDays") Integer numDays) {
        return db.queryItem(userID, startDay, 0, startDay + numDays - 1, 23);			

    }

    @POST
    @Path("deleteAll/{readCapacity}/{writeCapacity}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteAll(@PathParam("readCapacity") Integer readCapacity, @PathParam("writeCapacity") Integer writeCapacity) {
        return db.deleteAll(readCapacity, writeCapacity);
    }
    
    @POST
    @Path("dropTable")
    @Produces(MediaType.APPLICATION_JSON)
    public Response dropTable() {        
        return db.dropTable();
    }
    
    @POST
    @Path("createTable/{readCapacity}/{writeCapacity}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response createTable(@PathParam("readCapacity") Integer readCapacity, @PathParam("writeCapacity") Integer writeCapacity) {
        return db.createTable(readCapacity, writeCapacity);
    }
    
}