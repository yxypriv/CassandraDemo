package db;

import java.util.ArrayList;
import java.util.List;

import db.daos.SongsDAO;
import db.models.Songs;
import utils.connectionPool.ConnectionPool;

public class TestMain {
	static void testSongs() {
		SongsDAO dao = SongsDAO.getInstance();
		List<Songs> selectAll = dao.selectAll();
		for(Songs s : selectAll) {
			StringBuilder sb = new StringBuilder();
			sb.append(s.id).append("\t");
			sb.append("{");
			for(String tag : s.tags) {
				sb.append(tag).append(", ");
			}
			sb.delete(sb.length()-2, sb.length());
			sb.append("}");
			System.out.println(sb.toString());
		}
		System.out.println("--------------------------------------");
		List<String> content = new ArrayList<String>();
		content.add("at1");
		content.add("at2");
		dao.setRemoveByID(1, "tags", content);
		selectAll = dao.selectAll();
		for(Songs s : selectAll) {
			StringBuilder sb = new StringBuilder();
			sb.append(s.id).append("\t");
			sb.append("{");
			for(String tag : s.tags) {
				sb.append(tag).append(", ");
			}
			sb.delete(sb.length()-2, sb.length());
			sb.append("}");
			System.out.println(sb.toString());
		}
	}
	
	public static void main(String[] args) {
		testSongs();
		ConnectionPool.getInstance().close();
	}
}	
