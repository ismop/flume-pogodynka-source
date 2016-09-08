package pl.cyfronet.ismop.flume;

import java.io.IOException;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pl.cyfronet.ismop.flume.events.MomEncoderDecoder;
import pl.cyfronet.ismop.flume.events.MomEvent;
import pl.cyfronet.ismop.flume.events.MomEvent.Builder;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class PogodynkaSource extends AbstractSource implements Configurable,
		PollableSource {

	private static final Logger logger = LoggerFactory.getLogger(PogodynkaSource.class);
	
	private static final String POGODYNKA_SENSOR_ID_KEY = "sensor.id";
	private static final String POGODYNKA_URL_KEY = "pogodynka.url";
	private static final String DAP_CUSTOM_ID_KEY = "dap.custom.id";
	
	private String pogodynkaUrl;
	private String pogodynkaSensorId;
	private String dapCustomId;
	
	private MomEncoderDecoder encoder;
	private Pogodynka pogodynka;

	public PogodynkaSource() {
		super();
		encoder = new MomEncoderDecoder();
	}

	@Override
	public void configure(Context context) {
		pogodynkaSensorId = context.getString(POGODYNKA_SENSOR_ID_KEY, "149190230");
		pogodynkaUrl = context.getString(POGODYNKA_URL_KEY, "http://monitor.pogodynka.pl/api/station/hydro/");
		dapCustomId = context.getString(DAP_CUSTOM_ID_KEY, "POGODYNKA_149190230");
	}

	@Override
	public synchronized void start() {
		pogodynka = new Pogodynka();
		pogodynka.setSensorId(pogodynkaSensorId);
		pogodynka.setUrl(pogodynkaUrl);
		super.start();
	}

	@Override
	public synchronized void stop() {
		pogodynka.close();
		super.stop();
	}

	@Override
	public Status process() throws EventDeliveryException {
		Status status = null;
		try {
			Reading reading = pogodynka.nextReading();
			Event event = prepareEvent(reading);
			getChannelProcessor().processEvent(event);
			logger.debug("Pogodynka source event processed: ", event);
			status = Status.READY;
		} catch (Throwable t) {
			logger.warn("Pogodynka source event processing failed", t);
			status = Status.BACKOFF;
			if (t instanceof Error) {
				throw (Error) t;
			}
		} 
		return status;
	}
	
	private Event prepareEvent(Reading reading) throws IOException {
		Builder builder = MomEvent.newBuilder();
		MomEvent momEvent = builder.setTimestamp(reading.getTimestamp().getTime())
				.setSensorId(dapCustomId)
				.setValue(reading.getValue())
				.build();
		Event event = EventBuilder.withBody(encoder.encode(momEvent));
		return event;
	}

}
