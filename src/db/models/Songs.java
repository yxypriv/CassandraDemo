package db.models;

import java.nio.ByteBuffer;
import java.util.Set;

public class Songs {
	public Integer id;
	public String album;
	public String artist;
	public ByteBuffer data;
	public Set<String> tags;
	public String title;
}
