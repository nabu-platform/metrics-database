package be.nabu.libs.metrics.database.api;

public interface PartitionConfigurationProvider {
	public long getPartitionInterval(String id, String category);
	public long getPartitionSize(String id, String category);
}
