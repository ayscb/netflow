package cai.flow.packets.v9;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.Properties;

import cai.utils.Params;
import cai.utils.Util;
import cai.utils.DoneException;

/**
 * 
 *
 * @author CaiMao
 *
 */
public class Template {
	final static int MAX_TYPE = 93;

	private int samplingRate = 1;// init to 1

	static String templatePath = Params.path + "/../etc/templates/";

	private int wholeOffset = 0;

	Properties prop = new Properties();// persistent store

	int[] offsetOftypes = new int[MAX_TYPE];// runtime fetch

	int[] lenOftypes = new int[MAX_TYPE];// runtime fetch

	int templateId = 0;

	String routerIp = null;

	/**
	 * @return Returns the routerIp.
	 */
	public String getRouterIp() {
		return routerIp;
	}

	/**
	 * @param routerIp
	 *            The routerIp to set.
	 */
	public void setRouterIp(String routerIp) {
		this.routerIp = routerIp;
	}

	/**
	 *
	 * @param fileName
	 * @throws Exception
	 */
	public Template(String fileName) throws Exception {
		int beginIdx = fileName.lastIndexOf("\\");
		if (beginIdx < 0) {
			beginIdx = 0;
		} else {
			beginIdx += 1;
		}
		String routerIp = fileName.trim().substring(beginIdx,
				fileName.indexOf("_"));
		String templateIdStr = fileName.trim().substring(
				fileName.indexOf("_") + 1, fileName.lastIndexOf("."));
		int tid = Integer.parseInt(templateIdStr);
		makeTemplate(routerIp, tid);
	}

	/**
	 *
	 * @param routerIp
	 * @param tid
	 * @throws Exception
	 */
	public Template(String routerIp, int tid) throws Exception {
		makeTemplate(routerIp, tid);
	}

	/**
	 *
	 * @param routerIp
	 * @param tid
	 * @throws Exception
	 */
	public void makeTemplate(String routerIp, int tid) throws Exception {
		this.routerIp = routerIp;
		this.templateId = tid;
		String fullName = null;
		if (routerIp.indexOf(File.separator) == -1) {
                        System.out.println("Params.path = " + Params.path);
                        System.out.println("TemplatePath = " + templatePath);
			fullName = templatePath + routerIp;
                        System.out.println("Full name = " + fullName);
		} else {
			fullName = routerIp;
		}
		File propFile = new File(fullName + "_" + tid + ".properties");
		if (propFile.exists()) {
			InputStream propIn = new FileInputStream(propFile);
			prop.load(propIn);
		} else {
			System.err.println(propFile.getName() + " does not exist.");
		}
		//wholeOffset = Integer.parseInt(prop.getProperty("-1"));
		if (prop != null) {
                    wholeOffset = Integer.parseInt(prop.getProperty("-1"));
			for (Enumeration theKeys = prop.propertyNames(); theKeys
					.hasMoreElements();) {
				String key = theKeys.nextElement().toString();
				int typeName = Integer.parseInt(key);
				if (typeName > 0 && typeName < Template.MAX_TYPE) {
					int offset = Integer.parseInt(prop.getProperty(key));
					this.offsetOftypes[typeName] = offset;
					this.lenOftypes[typeName] = wholeOffset - offset;
				}
			}
			for (Enumeration theKeys = prop.propertyNames(); theKeys
					.hasMoreElements();) {
				String key = theKeys.nextElement().toString();
				int typeName = Integer.parseInt(key);
				if (typeName > 0 && typeName < Template.MAX_TYPE) {
					if (typeName == 11) {
						System.out.println("");
					}
					for (int i = 0; i < offsetOftypes.length; i++) {
						if (offsetOftypes[i] >= 0
								&& (offsetOftypes[i] - offsetOftypes[typeName] > 0)
								&& (offsetOftypes[i] - offsetOftypes[typeName] < lenOftypes[typeName])) {
							lenOftypes[typeName] = offsetOftypes[i]
									- offsetOftypes[typeName];
						}
					}
				}
			}
		}
	}

	/**
	 * ����һ��template������д�����
	 *
	 * @param routerIp
	 * @param pr
	 * @param tid
	 */

	// private Template(String routerIp, Properties pr, int tid) throws
	// Exception {
	// makeTemplate(routerIp, pr, tid);
	// }
	public void makeTemplate(String routerIp, Properties pr, int tid)
			throws Exception {
		prop = pr;
		templateId = tid;
		setRouterIp(routerIp);
		if (prop != null) {
			File propFile = new File(templatePath + routerIp + "_" + tid
					+ ".properties");
			if (propFile.exists()) {
				propFile.delete();
			}
			OutputStream propOut = new FileOutputStream(propFile);
			prop.store(propOut, "template of " + tid + " " + routerIp);
                        propOut.flush();
                        propOut.close();
		} else {
			throw new Exception("Template����Ϊ��");
		}
	}

	/**
	 * �ö��������һ��template
	 *
	 * @param routerIp
	 * @param flowset
	 * @param templateOffset
	 *            ��templateId ��ʼ
	 * @throws Exception
	 */
	public Template(String routerIp, byte[] flowset, int templateOffset)
			throws Exception {
		int tid = (int)Util.to_number(flowset, templateOffset,2);
		if (tid < 0 || tid > 255) {// 0-255 reserved for flowset IDs
			int fieldCnt = (int)Util.to_number(flowset, templateOffset + 2,2);
			Properties prop = new Properties();
			templateOffset += 4;
//			int dataFlowSetOffset = 4;// after the flowSetID and length
			//���������data record�ڲ���ƫ�ƣ�һ���Ǵ�0��ʼ
			int dataFlowSetOffset = 0;
			for (int idx = 0; idx < fieldCnt; idx++) {
				int typeName = (int)Util.to_number(flowset, templateOffset,2);
				templateOffset += 2;
				int typeLen = (int)Util.to_number(flowset, templateOffset,2);
				templateOffset += 2;
				if (typeName < MAX_TYPE && typeName > 0) {
					prop.setProperty(new Integer(typeName).toString(),
							new Integer(dataFlowSetOffset).toString());
					this.offsetOftypes[typeName] = dataFlowSetOffset;
					lenOftypes[typeName] = typeLen;
				}
				dataFlowSetOffset += typeLen;
			}
			if (prop.size() <= 0) {// if nothing is inputted
				throw new Exception("FieldType�Ƿ�");
			}
			// ��-1��Ϊkey��ǽ���Ҳ���Ǳ���ܳ���offset
			prop.setProperty(new Integer(-1).toString(), new Integer(
					dataFlowSetOffset).toString());
			wholeOffset = dataFlowSetOffset;
			this.makeTemplate(routerIp, prop, tid);
//                        if (tid==256){
//                            throw new DoneException("savePacketT_"+routerIp+"_"+tid);
//                        }
		} else {
			throw new Exception("templateID�Ƿ�");
		}
	}

	/**
	 * ����-1��ʾ���
	 *
	 * @param typeName
	 * @return
	 */
	public int getTypeOffset(int typeName) {
		if (typeName > 0 && typeName < MAX_TYPE) {
			if (this.offsetOftypes[typeName] == 0) {
				String value = this.prop.getProperty(new Integer(typeName)
						.toString());
				if (value != null) {
					offsetOftypes[typeName] = Integer.parseInt(value);
				}
			}
			return offsetOftypes[typeName];
		} else if (typeName == -1) {
			return wholeOffset;
		} else {
			return -1;// ���ﷵ��0���ܻ��������
		}
	}

	public int getTypeLen(int typeName) {
		if (typeName > 0 && typeName < MAX_TYPE) {
			return lenOftypes[typeName];
		}
		return 0;
	}

	/**
	 * @return Returns the templateId.
	 */
	public int getTemplateId() {
		return templateId;
	}

	/**
	 * @param templateId
	 *            The templateId to set.
	 */
	public void setTemplateId(int templateId) {
		this.templateId = templateId;
	}

	/**
	 * @return Returns the samplingRate.
	 */
	public int getSamplingRate() {
		return samplingRate;
	}

	/**
	 * @param samplingRate
	 *            The samplingRate to set.
	 */
	public void setSamplingRate(int samplingRate) {
		this.samplingRate = samplingRate;
	}

	/**
	 * @return Returns the wholeOffset.
	 */
	public int getWholeOffset() {
		return wholeOffset;
	}

	/**
	 * @param wholeOffset
	 *            The wholeOffset to set.
	 */
	public void setWholeOffset(int wholeOffset) {
		this.wholeOffset = wholeOffset;
	}
}
