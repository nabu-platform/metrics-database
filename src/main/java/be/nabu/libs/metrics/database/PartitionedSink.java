package be.nabu.libs.metrics.database;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import be.nabu.libs.metrics.core.SinkSnapshotImpl;
import be.nabu.libs.metrics.core.SinkValueComparator;
import be.nabu.libs.metrics.core.SinkValueImpl;
import be.nabu.libs.metrics.core.api.HistorySink;
import be.nabu.libs.metrics.core.api.SinkSnapshot;
import be.nabu.libs.metrics.core.api.SinkValue;
import be.nabu.libs.metrics.core.api.SinkStatistics;
import be.nabu.libs.metrics.core.api.StatisticsContainer;
import be.nabu.libs.metrics.core.sinks.StatisticsSink;
import be.nabu.libs.resources.ResourceUtils;
import be.nabu.libs.resources.api.ManageableContainer;
import be.nabu.libs.resources.api.ReadableResource;
import be.nabu.libs.resources.api.WritableResource;
import be.nabu.libs.resources.api.Resource;
import be.nabu.libs.resources.api.ResourceContainer;
import be.nabu.utils.io.IOUtils;
import be.nabu.utils.io.api.ByteBuffer;
import be.nabu.utils.io.api.ReadableContainer;
import be.nabu.utils.io.api.WritableContainer;

public class PartitionedSink implements HistorySink, StatisticsContainer {

	private static final int WINDOW = 100;
	
	private ResourceContainer<?> root;
	private long partitionInterval, partitionSize;
	private String category;
	private String id;
	private SinkSnapshot current;
	private Resource temporary;
	private StatisticsSink statistics;
	private Properties properties;
	private static ThreadLocal<SimpleDateFormat> formatter = new ThreadLocal<SimpleDateFormat>();

	PartitionedSink(PartitionedSinkProvider provider, String id, String category, ResourceContainer<?> root, long partitionInterval, long partitionSize) throws IOException, ParseException {
		this.id = id;
		this.category = category;
		this.root = root;
		this.partitionInterval = partitionInterval;
		this.partitionSize = partitionSize;
		this.statistics = new StatisticsSink(WINDOW);
		// if we have a temporary buffer, load from there
		if (provider.getTemporary() != null) {
			this.temporary = provider.getTemporary().getChild(id + ":" + category + ".csv");
			if (this.temporary == null) {
				this.temporary = ((ManageableContainer<?>) provider.getTemporary()).create(id + ":" + category + ".csv", "text/csv");
			}
			else {
				this.current = ResourceManager.load(this.temporary, false);
			}
		}
		if (this.current == null) {
			this.current = new SinkSnapshotImpl();
		}
		// load the last values into the statistics
		SinkSnapshot snapshot = getSnapshotUntil(WINDOW, new Date().getTime());
		for (SinkValue value : snapshot.getValues()) {
			statistics.push(value.getTimestamp(), value.getValue());
		}
	}
	
	public String getTag(String key) {
		return getProperties().getProperty(key);
	}
	
	public void setTag(String key, String value) {
		if (value == null) {
			getProperties().remove(key);
		}
		else {
			getProperties().setProperty(key, value);
		}
		saveProperties();
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Collection<String> getTags() {
		Set keySet = getProperties().keySet();
		return keySet;
	}
	
	private Properties getProperties() {
		if (properties == null) {
			try {
				synchronized(this) {
					if (properties == null) {
						Properties properties = new Properties();
						Resource child = root.getChild("meta.cfg");
						if (child instanceof ReadableResource) {
							ReadableContainer<ByteBuffer> readable = ((ReadableResource) child).getReadable();
							try {
								properties.load(IOUtils.toInputStream(readable));
							}
							finally {
								readable.close();
							}
						}
						this.properties = properties;
					}
				}
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		return properties;
	}
	
	private void saveProperties() {
		try {
			Properties properties = getProperties();
			Resource child = root.getChild("meta.cfg");
			if (child == null) {
				child = ((ManageableContainer<?>) root).create("meta.cfg", "text/plain");
			}
			WritableContainer<ByteBuffer> writable = ((WritableResource) child).getWritable();
			try {
				properties.store(IOUtils.toOutputStream(writable, true), "");
			}
			finally {
				writable.close();
			}
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public synchronized void push(long timestamp, long value) {
		try {
			List<SinkValue> values = current.getValues();
			// the value is _before_ this partition, we currently can not handle historical data being pushed
			if (!values.isEmpty() && timestamp < values.get(0).getTimestamp()) {
				throw new IllegalArgumentException("Can not push historical data that predates the partition for '" + id + "' category '" + category + "': " + timestamp + " < " + values.get(0).getTimestamp());
			}
			// the value is after the start of the partition but before the end, we still do not support this because we want to go for "append" instead of reordering
			else if (!values.isEmpty() && timestamp < values.get(values.size() - 1).getTimestamp()) {
				throw new IllegalArgumentException("Can not push historical data that is out of order within the current partition for '" + id + "' category '" + category + "': " + timestamp + " < " + values.get(values.size() - 1).getTimestamp());
			}
			// the value falls outside of this partition, create a new one
			else if ((partitionSize > 0 && values.size() >= partitionSize) || (!values.isEmpty() && timestamp > values.get(0).getTimestamp() + partitionInterval)) {
				String path = getFormatter().format(new Date(values.get(0).getTimestamp()));
				ResourceContainer<?> directory = ResourceUtils.mkdirs(root, path);
				Resource create = ((ManageableContainer<?>) directory).create(values.get(0).getTimestamp() + ".csv.gz", "text/csv");
				ResourceManager.save(create, current, true);
				// reset current
				current = new SinkSnapshotImpl();
				// reset temporary
				ResourceManager.save(temporary, current, false);
			}
			else {
				final SinkValue sinkValue = new SinkValueImpl(timestamp, value);
				values.add(sinkValue);
				ResourceManager.append(temporary, new SinkSnapshot() {
					@Override
					public List<SinkValue> getValues() {
						return Arrays.asList(sinkValue);
					}
				}, false);
			}
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
		catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public SinkSnapshot getSnapshotUntil(int amount, long until) {
		final List<SinkValue> values = new ArrayList<SinkValue>();
		List<SinkValue> currentValues = current.getValues();
		
		if (!currentValues.isEmpty()) {
			for (SinkValue currentValue : currentValues) {
				// we have enough values
				if (values.size() >= amount) {
					break;
				}
				else if (currentValue.getTimestamp() <= until) {
					values.add(currentValue);
				}
				else {
					break;
				}
			}
		}
		// if we don't have enough values yet, look through the history files
		if (values.size() < amount) {
			amount -= currentValues.size();
			Calendar calendar = Calendar.getInstance();
			Date currentDate = new Date();
			calendar.setTime(currentDate);
			try {
				int notFound = 0;
				getValues : while(values.size() < amount) {
					String path = getFormatter().format(calendar.getTime());
					ResourceContainer<?> fromFolder = (ResourceContainer<?>) ResourceUtils.resolve(root, path);
					if (fromFolder != null) {
						List<Resource> resources = getResources(0, currentDate.getTime(), fromFolder);
						Collections.reverse(resources);
						for (Resource resource : resources) {
							SinkSnapshot load = ResourceManager.load(resource, true);
							List<SinkValue> historicValues = load.getValues();
							if (historicValues.size() >= amount) {
								values.addAll(historicValues.subList(historicValues.size() - amount, historicValues.size()));
								break getValues;
							}
							else {
								values.addAll(historicValues);
								amount -= historicValues.size();
							}
						}
					}
					else {
						notFound++;
					}
					// if there is a gap of 30 days (= 1 month), stop searching 
					if (notFound > 30) {
						break;
					}
					calendar.add(Calendar.DATE, -1);
				}
			}
			catch (ParseException e) {
				throw new RuntimeException(e);
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
			// reorder the values
			Collections.sort(values, new SinkValueComparator<SinkValue>());
		}
		return new SinkSnapshotImpl(values, false);
	}

	@Override
	public SinkSnapshot getSnapshotBetween(long from, long until) {
		try {
			final List<SinkValue> values = new ArrayList<SinkValue>();
			// if we want data that is outside the current window, go to the historized data
			List<SinkValue> currentValues = current.getValues();
			if (currentValues.isEmpty() || from < currentValues.get(0).getTimestamp()) {
				Date fromDate = new Date(from);
				Date untilDate = new Date(until);
				checkContainers: for (ResourceContainer<?> container : listContainers(fromDate, untilDate)) {
					List<Resource> resources = getResources(from, until, container);
					for (Resource resource : resources) {
						SinkSnapshot load = ResourceManager.load(resource, true);
						for (SinkValue value : load.getValues()) {
							if (value.getTimestamp() >= from && value.getTimestamp() <= until) {
								values.add(value);
							}
							else if (value.getTimestamp() > until) {
								break checkContainers;
							}
						}
					}
				}
			}
			// if we also want value from the current window, add it
			if (!currentValues.isEmpty() && until >= currentValues.get(0).getTimestamp()) {
				for (SinkValue value : currentValues) {
					if (value.getTimestamp() >= from && value.getTimestamp() <= until) {
						values.add(value);
					}
					else if (value.getTimestamp() > until) {
						break;
					}
				}
			}
			return new SinkSnapshotImpl(values, false);
		}
		catch (ParseException e) {
			throw new RuntimeException(e);
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private List<Resource> getResources(long from, long until, ResourceContainer<?> container) {
		List<Resource> resources = new ArrayList<Resource>();
		for (Resource resource : container) {
			String name = resource.getName();
			int index = name.indexOf('.');
			if (index < 0) {
				continue;
			}
			String substring = name.substring(0, index);
			long timestamp = Long.parseLong(substring);
			if (timestamp >= from && timestamp <= until) {
				resources.add(resource);
			}
		}
		Collections.sort(resources, new Comparator<Resource>() {
			@Override
			public int compare(Resource o1, Resource o2) {
				return o1.getName().compareTo(o2.getName());
			}
		});
		return resources;
	}
	
	private List<ResourceContainer<?>> listContainers(Date from, Date to) throws IOException {
		List<ResourceContainer<?>> containers = new ArrayList<ResourceContainer<?>>();
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(from);
		while(from.before(to)) {
			String path = getFormatter().format(from);
			ResourceContainer<?> fromFolder = (ResourceContainer<?>) ResourceUtils.resolve(root, path);
			if (fromFolder != null) {
				containers.add(fromFolder);
			}
			calendar.add(Calendar.DATE, 1);
			from = calendar.getTime();
		}
		return containers;
	}
	
	private static SimpleDateFormat getFormatter() {
		if (formatter.get() == null) {
			formatter.set(new SimpleDateFormat("yyyy/MM/dd"));
		}
		return formatter.get();
	}

	public String getCategory() {
		return category;
	}

	public String getId() {
		return id;
	}

	@Override
	public SinkStatistics getStatistics() {
		return statistics;
	}
}
