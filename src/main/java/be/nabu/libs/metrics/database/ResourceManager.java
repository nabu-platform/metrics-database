package be.nabu.libs.metrics.database;

import java.io.IOException;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import be.nabu.libs.metrics.core.SinkValueImpl;
import be.nabu.libs.metrics.core.api.SinkSnapshot;
import be.nabu.libs.metrics.core.api.SinkValue;
import be.nabu.libs.resources.api.AppendableResource;
import be.nabu.libs.resources.api.ReadableResource;
import be.nabu.libs.resources.api.WritableResource;
import be.nabu.libs.resources.api.Resource;
import be.nabu.utils.codec.TranscoderUtils;
import be.nabu.utils.codec.impl.GZIPDecoder;
import be.nabu.utils.codec.impl.GZIPEncoder;
import be.nabu.utils.io.IOUtils;
import be.nabu.utils.io.api.ByteBuffer;
import be.nabu.utils.io.api.CharBuffer;
import be.nabu.utils.io.api.ReadableContainer;
import be.nabu.utils.io.api.WritableContainer;

public class ResourceManager {
	
	public static SinkSnapshot load(Resource resource, boolean zipped) throws IOException, ParseException {
		final List<SinkValue> values = new ArrayList<SinkValue>();
		ReadableContainer<ByteBuffer> bytes = ((ReadableResource) resource).getReadable();
		if (zipped) {
			bytes = TranscoderUtils.wrapReadable(bytes, new GZIPDecoder());
		}
		ReadableContainer<CharBuffer> readable = IOUtils.wrapReadable(bytes, Charset.forName("ASCII"));
		try {
			String line = null;
			int lineCounter = 0;
			while ((line = toString(IOUtils.delimit(readable, "\n"))) != null) {
				int index = line.indexOf(',');
				if (index < 0) {
					throw new ParseException("Invalid line [" + lineCounter + "]: " + line, lineCounter);
				}
				values.add(new SinkValueImpl(Long.parseLong(line.substring(0, index)), Long.parseLong(line.substring(index + 1))));
				lineCounter++;
			}
			
			return new SinkSnapshot() {
				@Override
				public List<SinkValue> getValues() {
					return values;
				}
			};
		}
		finally {
			readable.close();
		}
	}
	
	public static void append(Resource resource, SinkSnapshot snapshot, boolean zipped) throws IOException, ParseException {
		// this is the suboptimal append which loads the entire file then adds and resaves
		if (!(resource instanceof AppendableResource) || zipped) {
			SinkSnapshot current = load(resource, zipped);
			current.getValues().addAll(snapshot.getValues());
			save(resource, current, zipped);
		}
		// this is the optimal append where it is truely appended to the end
		else {
			WritableContainer<ByteBuffer> bytes = ((AppendableResource) resource).getAppendable();
			WritableContainer<CharBuffer> writable = IOUtils.wrapWritable(bytes, Charset.forName("ASCII"));
			try {
				for (SinkValue value : snapshot.getValues()) {
					writable.write(IOUtils.wrap(value.getTimestamp() + "," + value.getValue() + "\n"));
				}
			}
			finally {
				writable.close();
			}
		}
	}
	
	public static void save(Resource resource, SinkSnapshot snapshot, boolean zip) throws IOException {
		WritableContainer<ByteBuffer> bytes = ((WritableResource) resource).getWritable();
		if (zip) {
			bytes = TranscoderUtils.wrapWritable(bytes, new GZIPEncoder());
		}
		WritableContainer<CharBuffer> writable = IOUtils.wrapWritable(bytes, Charset.forName("ASCII"));
		try {
			for (SinkValue value : snapshot.getValues()) {
				writable.write(IOUtils.wrap(value.getTimestamp() + "," + value.getValue() + "\n"));
			}
		}
		finally {
			writable.close();
		}
	}

	private static String toString(ReadableContainer<CharBuffer> readable) throws IOException {
		char [] stringificationBuffer = new char[4096];
		StringBuilder builder = new StringBuilder();
		long read = 0;
		while ((read = readable.read(IOUtils.wrap(stringificationBuffer, false))) > 0) {
			builder.append(new String(stringificationBuffer, 0, (int) read));
		}
		String string = builder.toString();
		return string.isEmpty() ? null : string;
	}
}
