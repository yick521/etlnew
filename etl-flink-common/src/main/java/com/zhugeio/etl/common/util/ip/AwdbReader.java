package com.zhugeio.etl.common.util.ip;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * AWDB格式 IP数据库读取器
 * 支持 IPv4 和 IPv6 (来自 ipplus360.com)
 * 
 * 完整照搬 Scala: AWReader.java
 */
public class AwdbReader implements Serializable {
    
    private static final Logger LOG = LoggerFactory.getLogger(AwdbReader.class);
    private static final long serialVersionUID = 1L;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    // AWDB 元数据
    private final AwdbMetaData metadata;
    private final byte[] data;
    private transient ByteBuffer buffer;
    
    // 缓存
    private final Map<Integer, JsonNode> cache = new HashMap<>();

    public AwdbReader(byte[] data) throws IOException {
        this.data = data;
        this.buffer = ByteBuffer.wrap(data);
        this.metadata = parseMetadata();
        LOG.info("AWDB数据库初始化完成: ipVersion={}, nodeCount={}, decodeType={}", 
                metadata.getIpVersion(), metadata.getNodeCount(), metadata.getDecodeType());
    }

    /**
     * 查询IP地址，返回 [country, province, city]
     * 
     * 对应 Scala: AWReader.get(InetAddress)
     */
    public String[] get(InetAddress ipAddress) throws IOException {
        String country = "";
        String prov = "";
        String city = "";

        try {
            if (ipAddress instanceof Inet4Address) {
                // IPv4 地址查询
                JsonNode value = findIpLocation(ipAddress);
                if (value != null) {
                    country = getTextValue(value, "country");
                    // IPv4 格式: multiAreas[0].prov, multiAreas[0].city
                    JsonNode multiAreas = value.get("multiAreas");
                    if (multiAreas != null && multiAreas.isArray() && multiAreas.size() > 0) {
                        JsonNode area = multiAreas.get(0);
                        prov = getTextValue(area, "prov");
                        city = getTextValue(area, "city");
                    }
                }
            } else if (ipAddress instanceof Inet6Address) {
                // IPv6 地址查询
                JsonNode value = findIpLocation(ipAddress);
                if (value != null) {
                    country = getTextValue(value, "country");
                    // IPv6 格式: province, city (直接字段)
                    prov = getTextValue(value, "province");
                    city = getTextValue(value, "city");
                }
                LOG.debug("IPv6查询: {} -> {},{},{}", ipAddress.getHostAddress(), country, prov, city);
            }
        } catch (Exception e) {
            LOG.error("AWDB查询异常: {}", ipAddress, e);
        }

        return new String[]{country, prov, city};
    }

    /**
     * 查找IP地址位置
     * 
     * 对应 Scala: AWReader.findIpLocation
     */
    private JsonNode findIpLocation(InetAddress ipAddr) throws IOException {
        ensureBuffer();
        
        int nodeIndex = findTreeIndex(ipAddr);
        
        if (nodeIndex == 0) {
            return null;
        }

        int pointer = metadata.getBaseOffset() + nodeIndex - metadata.getNodeCount() - 10;
        
        switch (metadata.getDecodeType()) {
            case 1:
                return decodeContentStructure(pointer);
            case 2:
                return decodeContentDirect(pointer);
            default:
                LOG.warn("无效的解码类型: {}", metadata.getDecodeType());
                return null;
        }
    }

    /**
     * 查找数据索引 (二叉树遍历)
     * 
     * 对应 Scala: AWReader.findTreeIndex
     */
    private int findTreeIndex(InetAddress ipAddress) throws IOException {
        byte[] rawAddr = ipAddress.getAddress();
        int bitLength = rawAddr.length * 8;
        int nodeCount = metadata.getNodeCount();
        int nodeIndex = 0;

        for (int pl = 0; pl < bitLength && nodeIndex < nodeCount; pl++) {
            int b = 0xFF & rawAddr[pl / 8];
            int bit = 1 & (b >> 7 - (pl % 8));
            nodeIndex = readNodeIndex(nodeIndex, bit);
        }

        if (nodeIndex == nodeCount) {
            return 0;
        } else if (nodeIndex > nodeCount) {
            return nodeIndex;
        }

        return 0;
    }

    /**
     * 读取节点索引
     * 
     * 对应 Scala: AWReader.readNodeIndex
     */
    private int readNodeIndex(int nodeIndex, int bit) {
        int offset = nodeIndex * metadata.getByteLen() * 2 
                   + bit * metadata.getByteLen() 
                   + metadata.getStartLength();
        buffer.position(offset);
        return buffer2Integer(buffer, 0, metadata.getByteLen());
    }

    /**
     * 解析结构化内容 (decodeType=1)
     * 
     * 对应 Scala: AWReader.decodeContentStructure
     */
    private JsonNode decodeContentStructure(int offset) throws IOException {
        JsonNode valuesJson = parseDataPointer(offset);
        return mapKeyValue(metadata.getColumns(), valuesJson);
    }

    /**
     * 解析直接内容 (decodeType=2, \t分割)
     * 
     * 对应 Scala: AWReader.decodeContentDirect
     */
    private JsonNode decodeContentDirect(int offset) throws IOException {
        int position = buffer.position();
        buffer.position(offset);

        int dataLen = buffer2Integer(buffer, 0, 4);
        
        int oldLimit = buffer.limit();
        buffer.limit(offset + 4 + dataLen);
        
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
        String content;
        try {
            content = decoder.decode(buffer).toString();
        } finally {
            buffer.limit(oldLimit);
            buffer.position(position);
        }
        
        String[] values = content.split("\t");
        JSONArray columns = metadata.getColumns();
        
        Map<String, JsonNode> result = new HashMap<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            String value = (i < values.length) ? values[i] : "";
            result.put(columns.getString(i), new TextNode(value));
        }

        return new ObjectNode(OBJECT_MAPPER.getNodeFactory(), Collections.unmodifiableMap(result));
    }

    /**
     * 解析指针数据
     */
    private JsonNode parseDataPointer(int offset) throws IOException {
        AwdbDataParser parser = new AwdbDataParser(buffer, metadata.getBaseOffset());
        return parser.parseData(offset);
    }

    /**
     * key和value映射
     * 
     * 对应 Scala: AWReader.mapKeyValue
     */
    private JsonNode mapKeyValue(JSONArray keys, JsonNode values) {
        Map<String, JsonNode> resultDict = new HashMap<>(keys.size());

        if (keys.size() == values.size()) {
            for (int i = 0; i < keys.size(); i++) {
                resultDict.put(keys.getString(i), values.get(i));
            }
        } else {
            for (int i = 0; i < values.size() - 1; i++) {
                resultDict.put(keys.getString(i), values.get(i));
            }

            // 处理 multiAreas
            String multiAreasName = keys.getString(keys.size() - 2);
            JSONArray keysList = keys.getJSONArray(keys.size() - 1);
            JsonNode valuesJsonNode = values.get(values.size() - 1);

            ArrayNode nodes = new ArrayNode(OBJECT_MAPPER.getNodeFactory());
            for (JsonNode value : valuesJsonNode) {
                Map<String, JsonNode> tempDic = new HashMap<>(keysList.size());
                for (int i = 0; i < keysList.size(); i++) {
                    tempDic.put(keysList.getString(i), value.get(i));
                }
                nodes.add(new ObjectNode(OBJECT_MAPPER.getNodeFactory(), Collections.unmodifiableMap(tempDic)));
            }
            resultDict.put(multiAreasName, nodes);
        }

        return new ObjectNode(OBJECT_MAPPER.getNodeFactory(), Collections.unmodifiableMap(resultDict));
    }

    // ========== 元数据解析 ==========

    private AwdbMetaData parseMetadata() throws IOException {
        buffer.position(0);
        
        // 读取前2字节作为元数据长度
        int num = 0;
        for (int i = 0; i < 2; i++) {
            num = (num << 8) | (buffer.get() & 0xFF);
        }

        // 读取元数据JSON
        int startLen = 2 + num;
        buffer.limit(startLen);
        
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
        String metaStr = decoder.decode(buffer).toString();
        buffer.limit(data.length);
        
        JSONObject metaJson = JSONObject.parseObject(metaStr);
        return new AwdbMetaData(metaJson, startLen);
    }

    // ========== 辅助方法 ==========

    private void ensureBuffer() {
        if (buffer == null) {
            buffer = ByteBuffer.wrap(data);
        }
    }

    private String getTextValue(JsonNode node, String field) {
        if (node == null) {
            return "";
        }
        JsonNode child = node.get(field);
        if (child == null || child.isNull()) {
            return "";
        }
        String text = child.asText();
        return text == null ? "" : text.trim();
    }

    private static int buffer2Integer(ByteBuffer buffer, int base, int len) {
        int num = base;
        for (int i = 0; i < len; i++) {
            num = (num << 8) | (buffer.get() & 0xFF);
        }
        return num;
    }

    // ========== 内部类: AWDB 元数据 ==========

    /**
     * AWDB 文件元数据
     * 
     * 对应 Scala: AwdbMetaData.java
     */
    public static class AwdbMetaData implements Serializable {
        private static final long serialVersionUID = 1L;
        
        private final int nodeCount;
        private final String ipVersion;
        private final int decodeType;
        private final int byteLen;
        private final String languages;
        private final String fileName;
        private final String createTime;
        private final String companyId;
        private final int startLength;
        private final int baseOffset;
        private final JSONArray columns;

        public AwdbMetaData(JSONObject metaJson, int startLen) {
            this.nodeCount = metaJson.getIntValue("node_count");
            this.ipVersion = metaJson.getString("ip_version");
            this.decodeType = metaJson.getIntValue("decode_type");
            this.byteLen = metaJson.getIntValue("byte_len");
            this.languages = metaJson.getString("languages");
            this.fileName = metaJson.getString("file_name");
            this.createTime = metaJson.getString("create_time");
            this.companyId = metaJson.getString("company_id");
            this.startLength = startLen;
            this.baseOffset = nodeCount * byteLen * 2 + startLen;
            this.columns = metaJson.getJSONArray("columns");
        }

        public int getNodeCount() { return nodeCount; }
        public String getIpVersion() { return ipVersion; }
        public int getDecodeType() { return decodeType; }
        public int getByteLen() { return byteLen; }
        public String getLanguages() { return languages; }
        public String getFileName() { return fileName; }
        public String getCreateTime() { return createTime; }
        public String getCompanyId() { return companyId; }
        public int getStartLength() { return startLength; }
        public int getBaseOffset() { return baseOffset; }
        public JSONArray getColumns() { return columns; }
    }

    // ========== 内部类: AWDB 数据解析器 ==========

    /**
     * AWDB 文件数据解析器
     * 
     * 对应 Scala: AwdbDataParser.java
     */
    private class AwdbDataParser {
        private final ByteBuffer buffer;
        private final int basePointer;
        private final CharsetDecoder strDecoder = StandardCharsets.UTF_8.newDecoder();

        public AwdbDataParser(ByteBuffer buffer, int basePointer) {
            this.buffer = buffer;
            this.basePointer = basePointer;
        }

        public JsonNode parseData(int offset) throws IOException {
            if (offset >= buffer.capacity()) {
                throw new IOException("AWDB数据指针越界: " + offset);
            }

            // 检查缓存
            JsonNode cached = cache.get(offset);
            if (cached != null) {
                return cached;
            }

            buffer.position(offset);
            JsonNode result = parser();
            cache.put(offset, result);
            return result;
        }

        private JsonNode parser() throws IOException {
            int typeByte = 0xFF & buffer.get();
            AwdbDataType dataType = AwdbDataType.getDataType(typeByte);
            int len = 0xFF & buffer.get();
            return parseDataType(dataType, len);
        }

        private JsonNode parseDataType(AwdbDataType type, int len) throws IOException {
            switch (type) {
                case ARRAY:
                    return parseArray(len);
                case POINTER:
                    return parsePointer(len);
                case STRING:
                    return parseString(len);
                case TEXT:
                    return parseText(len);
                case INT:
                    return parseInt();
                case UINT:
                    return parseUint(len);
                case FLOAT:
                    return parseFloat();
                case DOUBLE:
                    return parseDouble();
                default:
                    throw new IOException("未知的AWDB数据类型: " + type.name());
            }
        }

        private JsonNode parseArray(int len) throws IOException {
            List<JsonNode> list = new ArrayList<>(len);
            for (int i = 0; i < len; i++) {
                list.add(parser());
            }
            return new ArrayNode(OBJECT_MAPPER.getNodeFactory()).addAll(list);
        }

        private JsonNode parsePointer(int len) throws IOException {
            int oldLimit = buffer.limit();
            buffer.limit(buffer.position() + len);
            int buf = buffer2Integer(buffer, 0, len);
            int pointer = basePointer + buf;
            buffer.limit(oldLimit);
            int position = buffer.position();
            
            // 检查缓存
            JsonNode cached = cache.get(pointer);
            if (cached != null) {
                buffer.position(position);
                return cached;
            }
            
            JsonNode result = parseData(pointer);
            buffer.position(position);
            return result;
        }

        private JsonNode parseString(int len) throws CharacterCodingException {
            int oldLimit = buffer.limit();
            buffer.limit(buffer.position() + len);
            String s = strDecoder.decode(buffer).toString();
            buffer.limit(oldLimit);
            return new TextNode(s);
        }

        private JsonNode parseText(int len) throws CharacterCodingException {
            int oldLimit = buffer.limit();
            buffer.limit(buffer.position() + len);
            int dataLen = buffer2Integer(buffer, 0, len);
            buffer.limit(oldLimit);
            buffer.limit(buffer.position() + dataLen);
            String s = strDecoder.decode(buffer).toString();
            buffer.limit(oldLimit);
            return new TextNode(s);
        }

        private JsonNode parseUint(int len) {
            long num = 0;
            for (int i = 0; i < len; i++) {
                num = (num << 8) | (buffer.get() & 0xFF);
            }
            return new LongNode(num);
        }

        private JsonNode parseInt() {
            buffer.position(buffer.position() - 1);
            return new IntNode(buffer2Integer(buffer, 0, 4));
        }

        private JsonNode parseDouble() {
            buffer.position(buffer.position() - 1);
            return new DoubleNode(buffer.getDouble());
        }

        private JsonNode parseFloat() {
            buffer.position(buffer.position() - 1);
            return new FloatNode(buffer.getFloat());
        }
    }

    // ========== 内部枚举: AWDB 数据类型 ==========

    /**
     * AWDB 数据类型
     * 
     * 对应 Scala: AwdbDataType.java
     */
    private enum AwdbDataType {
        ARRAY,      // 列表类型
        POINTER,    // 指针类型
        STRING,     // 字符串类型
        TEXT,       // 长文本类型
        UINT,       // 无符号int类型
        INT,        // 有符号int类型
        FLOAT,      // float类型
        DOUBLE;     // double类型

        private static final AwdbDataType[] VALUES = AwdbDataType.values();

        public static AwdbDataType getDataType(int b) {
            return VALUES[b - 1];
        }
    }
}
