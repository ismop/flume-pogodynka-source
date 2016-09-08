package pl.cyfronet.ismop.flume;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Pogodynka {

	private String url = "http://monitor.pogodynka.pl/api/station/hydro/";
	private String sensorId = "149190230";
	
	private Client client;
	
	public Pogodynka() {
		client = ClientBuilder.newClient();
	}
	
	public Reading nextReading() throws PogodynkaParseException, PogodynkaRequestFailedException {
		Response response = pogodynkaRequest();
		return parsePogodynkaResponse(response);		
	}
	
	public void close() {
		client.close();
	}
	
	private Reading parsePogodynkaResponse(Response response) throws PogodynkaParseException {
		try {
			ObjectMapper mapper = new ObjectMapper();
			JsonNode readTree = mapper.readTree(response.readEntity(String.class));
			
			JsonNode jsonNode = readTree.get("status");
			JsonNode dateNode = jsonNode.get("currentDate");
			JsonNode valueNode = jsonNode.get("currentValue");
	
			ZonedDateTime dateTime = ZonedDateTime.parse(dateNode.asText(), DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.systemDefault()));	
			Date date = Date.from(dateTime.toLocalDateTime().atZone(ZoneId.systemDefault()).toInstant());
			float value = Float.parseFloat(valueNode.asText());
			
			return new Reading(date, value);
		} catch (Exception e) {
			throw new PogodynkaParseException(e);
		}
	}

	private Response pogodynkaRequest() throws PogodynkaRequestFailedException {
		try {
			WebTarget target = client.target(url).queryParam("id", sensorId);
			Response response = target.
		              request().
		              accept(MediaType.APPLICATION_JSON).
		              get(Response.class);
			return response;
		} catch (Exception e){
			throw new PogodynkaRequestFailedException(e);
		}
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public void setSensorId(String sensorId) {
		this.sensorId = sensorId;
	}

	public static void main(String[] args) throws PogodynkaParseException, PogodynkaRequestFailedException {
		Reading readPogodynka = new Pogodynka().nextReading();
		System.out.println("timestamp: " + readPogodynka.getTimestamp());
		System.out.println("value: " + readPogodynka.getValue());
	}

};