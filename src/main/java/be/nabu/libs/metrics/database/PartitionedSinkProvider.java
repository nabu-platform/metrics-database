/*
* Copyright (C) 2016 Alexander Verbruggen
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Lesser General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public License
* along with this program. If not, see <https://www.gnu.org/licenses/>.
*/

package be.nabu.libs.metrics.database;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import be.nabu.libs.metrics.core.api.ListableSinkProvider;
import be.nabu.libs.metrics.database.api.PartitionConfigurationProvider;
import be.nabu.libs.resources.api.ManageableContainer;
import be.nabu.libs.resources.api.Resource;
import be.nabu.libs.resources.api.ResourceContainer;

public class PartitionedSinkProvider implements ListableSinkProvider {

	private ResourceContainer<?> root;
	private PartitionConfigurationProvider partitionConfigurationProvider;
	private ResourceContainer<?> temporary;
	private Map<String, PartitionedSink> sinks = new HashMap<String, PartitionedSink>();

	public PartitionedSinkProvider(PartitionConfigurationProvider partitionConfigurationProvider, ResourceContainer<?> root, ResourceContainer<?> temporary) {
		this.partitionConfigurationProvider = partitionConfigurationProvider;
		this.root = root;
		this.temporary = temporary;
	}
	
	@Override
	public PartitionedSink getSink(String id, String category) {
		try {
			String key = id + ":" + category;
			if (!sinks.containsKey(key)) {
				synchronized(this) {
					if (!sinks.containsKey(id + ":" + category)) {
						sinks.put(key, new PartitionedSink(
							this, 
							id,
							category, 
							getRootFor(id, category), 
							partitionConfigurationProvider.getPartitionInterval(id, category), 
							partitionConfigurationProvider.getPartitionSize(id, category)
						));
					}
				}
			}
			return sinks.get(key);
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
		catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}

	private ResourceContainer<?> getRootFor(String id, String category) {
		try {
			ResourceContainer<?> child = (ResourceContainer<?>) root.getChild(id);
			if (child == null) {
				child = (ResourceContainer<?>) ((ManageableContainer<?>) root).create(id, Resource.CONTENT_TYPE_DIRECTORY);
			}
			ResourceContainer<?> target = (ResourceContainer<?>) child.getChild(category);
			if (target == null) {
				target = (ResourceContainer<?>) ((ManageableContainer<?>) child).create(category, Resource.CONTENT_TYPE_DIRECTORY);
			}
			return target;
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public ResourceContainer<?> getRoot() {
		return root;
	}

	public PartitionConfigurationProvider getPartitionIntervalProvider() {
		return partitionConfigurationProvider;
	}

	public ResourceContainer<?> getTemporary() {
		return temporary;
	}

	@Override
	public Map<String, List<String>> getSinks() {
		Map<String, List<String>> sinks = new HashMap<String, List<String>>();
		for (Resource resource : root) {
			if (resource instanceof ResourceContainer) {
				String id = resource.getName();
				List<String> categories = new ArrayList<String>();
				for (Resource child : (ResourceContainer<?>) resource) {
					if (child instanceof ResourceContainer) {
						categories.add(child.getName());
					}
				}
				if (!categories.isEmpty()) {
					sinks.put(id, categories);
				}
			}
		}
		return sinks;
	}
}
