package io.rtdi.bigdata.connector.connectorframework.rest.entity;

import java.util.ArrayList;
import java.util.List;

public class BrowsingNode {

	public static class Condition {
		private String left;
		private String right;
		
		public Condition() {
		}
	
		public Condition(String left, String right) {
			this.left = left;
			this.right = right;
		}
	
		public String getLeft() {
			return left;
		}
		
		public void setLeft(String left) {
			this.left = left;
		}
		
		public String getRight() {
			return right;
		}
		
		public void setRight(String right) {
			this.right = right;
		}
		
		@Override
		public String toString() {
			return left + " = " + right;
		}
	
	}

	private String nodeid;
	private String displayname;
	private String from;
	private String to;
	private String description;
	private String nodetype;
	private boolean expandable;
	private boolean importable;
	private double nameweight;
	private List<BrowsingNode> children;
	private List<Condition> joincondition;
	
	public BrowsingNode() {
	}

	public BrowsingNode(String nodeid, String name, String desc, double nameweight) {
		this.nodeid = nodeid;
		this.displayname = name;
		this.description = desc;
		this.expandable = false;
		this.importable = true;
		this.nameweight = nameweight;
		this.from = name;
		this.to = name;
	}
	

	private void calculateCenter() {
		nameweight = 0.0;
		for (BrowsingNode e : children) {
			nameweight += e.nameweight;
		}
		nameweight = nameweight / children.size();
	}

	private int swapElements(BrowsingNode partner) {
		if (children.size() != 0) {
			BrowsingNode e1 = children.get(children.size()-1);
			double d1 = e1.nameweight - nameweight;
			double d2 = partner.nameweight - e1.nameweight;
			if (d1 > d2) {
			// if (d1 > d2 || children.size() > partner.children.size()+5) {
				children.remove(children.size()-1);
				partner.children.add(0, e1);
			} else {
				e1 = partner.children.get(0);
				d1 = e1.nameweight - nameweight;
				d2 = partner.nameweight - e1.nameweight;
				if (d1 < d2) {
				// if (d1 < d2 || children.size() < partner.children.size()-5) {
					children.add(e1);
					partner.children.remove(0);
				} else {
					return 0;
				}
			}
			return 1;
		} else {
			return 0;
		}
	}

	
	public static List<BrowsingNode> addAllSorted(List<BrowsingNode> sortedlist, int maxsize) {
		List<BrowsingNode> clusters = new ArrayList<>();
		// distribute the data into the clusters
		BrowsingNode c = null;
		for (int i = 0; i < sortedlist.size(); i++) {
			if (i % maxsize == 0) {
				c = new BrowsingNode();
				c.children = new ArrayList<>();
				c.setExpandable(true);
				c.setImportable(false);
				clusters.add(c);
			}
			c.children.add(sortedlist.get(i));
		}
		int iterations = 0; // failsafe
		int movedelements = 0;
		do {
			for (BrowsingNode d : clusters) {
				d.calculateCenter();
			}
			movedelements = 0;
			for (int i = 0; i < clusters.size()-1; i++) {
				movedelements += clusters.get(i).swapElements(clusters.get(i+1));
			}
			iterations++;
		} while (movedelements != 0 && iterations < 100);
		
		for (BrowsingNode d : clusters) {
			d.from = d.children.get(0).from;
			d.to = d.children.get(d.children.size()-1).to;
			d.nodeid = d.from + "..." + d.to;
			d.displayname = d.from + "........" + d.to;
		}
		if (clusters.size() > maxsize*2) {
			return addAllSorted(clusters, maxsize);
		} else {
			return clusters;
		}
	}

	public String getNodeid() {
		return nodeid;
	}

	public void setNodeid(String nodeid) {
		this.nodeid = nodeid;
	}

	public String getDisplayname() {
		return displayname;
	}

	public void setDisplayname(String displayname) {
		this.displayname = displayname;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getNodetype() {
		return nodetype;
	}

	public void setNodetype(String nodetype) {
		this.nodetype = nodetype;
	}

	public boolean isExpandable() {
		return expandable;
	}

	public void setExpandable(boolean expandable) {
		this.expandable = expandable;
	}

	public boolean isImportable() {
		return importable;
	}

	public void setImportable(boolean importable) {
		this.importable = importable;
	}

	public List<BrowsingNode> getChildren() {
		return children;
	}

	public void setChildren(List<BrowsingNode> children) {
		this.children = children;
	}

	@Override
	public String toString() {
		return "BrowsingNode [nodeid=" + nodeid + "]";
	}

	public void addJoinCondition(String left, String right) {
		if (joincondition == null) {
			joincondition = new ArrayList<>();
		}
		joincondition.add(new Condition(left, right));
	}

	public List<Condition> getJoincondition() {
		return joincondition;
	}

	public void setJoincondition(List<Condition> joincondition) {
		this.joincondition = joincondition;
	}

}
