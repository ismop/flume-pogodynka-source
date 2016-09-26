/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package pl.cyfronet.ismop.flume;

import java.time.ZoneId;
import java.time.ZoneOffset;
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
			
			ZonedDateTime dateTime = ZonedDateTime.parse(dateNode.asText(),
					DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneOffset.UTC));	
			Date date = Date.from(dateTime.toLocalDateTime()
					.atZone(ZoneOffset.UTC).toInstant());
			
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