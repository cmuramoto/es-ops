package com.nc.es.ops;

import java.io.IOException;
import java.util.ArrayList;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.nc.es.ops.MappedSearchResult.Hit;
import com.nc.es.ops.MappedSearchResult.Hits;

public class MappedSearchResultDeserializer<T> extends JsonDeserializer<MappedSearchResult<T>> {

	Class<T> type;

	public MappedSearchResultDeserializer(Class<T> type) {
		super();
		this.type = type;
	}

	@Override
	public MappedSearchResult<T> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
		final MappedSearchResult<T> rv = new MappedSearchResult<>();

		while (true) {
			String field;
			final JsonToken token = p.nextToken();
			if (token == JsonToken.FIELD_NAME) {
				field = p.getCurrentName();
			} else if (token == null || token == JsonToken.END_OBJECT) {
				break;
			} else {
				continue;
			}

			p.nextToken();
			switch (field) {
			case "_scroll_id":
				rv.scrollId = p.getValueAsString();
				break;
			case "took":
				rv.took = p.getValueAsInt();
				break;
			case "timed_out":
				rv.timedOut = p.getValueAsBoolean();
				break;
			case "_shards":
				rv.shards = doShards(p, ctxt);
				break;
			case "hits":
				rv.hits = doHits(p, ctxt);
				break;

			default:
				break;
			}
		}

		return rv;
	}

	private Hit<T> doHit(JsonParser p, DeserializationContext ctxt) throws IOException {
		final Hit<T> h = new Hit<>();
		loop: while (true) {
			String field;
			JsonToken token = p.nextToken();
			if (token == JsonToken.FIELD_NAME) {
				field = p.getCurrentName();
			} else if (token == null || token == JsonToken.END_OBJECT) {
				break;
			} else {
				continue;
			}

			token = p.nextToken();
			switch (field) {
			case "_id":
				h.id = p.getValueAsString();
				if (h.source != null) {
					break loop;
				}

				break;
			case "_source":
				if (token != JsonToken.START_OBJECT) {
					// error
					return h;
				}
				h.source = p.readValueAs(type);
				if (h.id != null) {
					break loop;
				}

			default:
				break;
			}

		}

		return h;
	}

	private Hits<T> doHits(JsonParser p, DeserializationContext ctxt) throws IOException {
		final Hits<T> rv = new Hits<>();

		while (true) {
			String field;
			JsonToken token = p.nextToken();
			if (token == JsonToken.FIELD_NAME) {
				field = p.getCurrentName();
			} else if (token == null || token == JsonToken.END_OBJECT) {
				break;
			} else {
				continue;
			}

			token = p.nextToken();
			switch (field) {
			case "total":
				rv.total = p.getValueAsInt();
				break;
			case "hits":
				rv.hits = new ArrayList<>();
				JsonToken tk = token;
				loop: while (tk != null) {
					switch (tk) {
					case END_ARRAY:
						break loop;
					case START_OBJECT:
						final Hit<T> hit = doHit(p, ctxt);
						if (hit.source != null) {
							rv.hits.add(hit);
						}
						break;
					default:
						break;
					}
					tk = p.nextToken();
				}
			default:
				break;
			}
		}
		return rv;
	}

	private ShardStats doShards(JsonParser p, DeserializationContext ctxt) throws IOException {
		final ShardStats ss = new ShardStats();
		while (true) {
			String field;
			final JsonToken token = p.nextToken();
			if (token == JsonToken.FIELD_NAME) {
				field = p.getCurrentName();
			} else if (token == null || token == JsonToken.END_OBJECT) {
				break;
			} else {
				continue;
			}

			p.nextToken();
			switch (field) {
			case "total":
				ss.total = p.getValueAsInt();
				break;
			case "successful":
				ss.successful = p.getValueAsInt();
				break;
			case "failed":
				ss.failed = p.getValueAsInt();
				break;
			default:
				break;
			}
		}

		return ss;
	}

}