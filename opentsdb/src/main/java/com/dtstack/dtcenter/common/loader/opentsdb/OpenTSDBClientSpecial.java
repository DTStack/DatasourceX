package com.dtstack.dtcenter.common.loader.opentsdb;

import com.dtstack.dtcenter.common.loader.opentsdb.tsdb.OpenTSDBConnFactory;
import com.dtstack.dtcenter.common.loader.opentsdb.tsdb.TSDB;
import com.dtstack.dtcenter.loader.client.ITsdb;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.tsdb.QueryResult;
import com.dtstack.dtcenter.loader.dto.tsdb.Suggest;
import com.dtstack.dtcenter.loader.dto.tsdb.TsdbPoint;
import com.dtstack.dtcenter.loader.dto.tsdb.TsdbQuery;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * OpenTSDB 特有客户端
 *
 * @author ：wangchuan
 * date：Created in 上午10:21 2021/6/23
 * company: www.dtstack.com
 */
public class OpenTSDBClientSpecial implements ITsdb {

    @Override
    public Boolean putSync(ISourceDTO source, Collection<TsdbPoint> points) {
        try (TSDB openTSDBClient = OpenTSDBConnFactory.getOpenTSDBClient(source)) {
            return openTSDBClient.putSync(points);
        } catch (IOException e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public List<QueryResult> query(ISourceDTO source, TsdbQuery query) {
        try (TSDB openTSDBClient = OpenTSDBConnFactory.getOpenTSDBClient(source)) {
            return openTSDBClient.query(query);
        } catch (IOException e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public Boolean deleteData(ISourceDTO source, String metric, long startTime, long endTime) {
        try (TSDB openTSDBClient = OpenTSDBConnFactory.getOpenTSDBClient(source)) {
            openTSDBClient.deleteData(metric, startTime, endTime);
            return true;
        } catch (IOException e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public Boolean deleteData(ISourceDTO source, String metric, Map<String, String> tags, long startTime, long endTime) {
        try (TSDB openTSDBClient = OpenTSDBConnFactory.getOpenTSDBClient(source)) {
            openTSDBClient.deleteData(metric, tags, startTime, endTime);
            return true;
        } catch (IOException e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public Boolean deleteData(ISourceDTO source, String metric, List<String> fields, long startTime, long endTime) {
        try (TSDB openTSDBClient = OpenTSDBConnFactory.getOpenTSDBClient(source)) {
            openTSDBClient.deleteData(metric, fields, startTime, endTime);
            return true;
        } catch (IOException e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public Boolean deleteData(ISourceDTO source, String metric, Map<String, String> tags, List<String> fields, long startTime, long endTime) {
        try (TSDB openTSDBClient = OpenTSDBConnFactory.getOpenTSDBClient(source)) {
            openTSDBClient.deleteData(metric, tags, fields, startTime, endTime);
            return true;
        } catch (IOException e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public Boolean deleteMeta(ISourceDTO source, String metric, Map<String, String> tags) {
        try (TSDB openTSDBClient = OpenTSDBConnFactory.getOpenTSDBClient(source)) {
            openTSDBClient.deleteMeta(metric, tags);
            return true;
        } catch (IOException e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public Boolean deleteMeta(ISourceDTO source, String metric, Map<String, String> tags, List<String> fields) {
        try (TSDB openTSDBClient = OpenTSDBConnFactory.getOpenTSDBClient(source)) {
            openTSDBClient.deleteMeta(metric, tags, fields);
            return true;
        } catch (IOException e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public Boolean deleteMeta(ISourceDTO source, String metric, Map<String, String> tags, boolean deleteData, boolean recursive) {
        try (TSDB openTSDBClient = OpenTSDBConnFactory.getOpenTSDBClient(source)) {
            openTSDBClient.deleteMeta(metric, tags, deleteData, recursive);
            return true;
        } catch (IOException e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public Boolean deleteMeta(ISourceDTO source, String metric, List<String> fields, Map<String, String> tags, boolean deleteData, boolean recursive) {
        try (TSDB openTSDBClient = OpenTSDBConnFactory.getOpenTSDBClient(source)) {
            openTSDBClient.deleteMeta(metric, fields, tags, deleteData, recursive);
            return true;
        } catch (IOException e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public List<String> suggest(ISourceDTO source, Suggest type, String prefix, int max) {
        try (TSDB openTSDBClient = OpenTSDBConnFactory.getOpenTSDBClient(source)) {
            return openTSDBClient.suggest(type, prefix, max);
        } catch (IOException e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public List<String> suggest(ISourceDTO source, Suggest type, String metric, String prefix, int max) {
        try (TSDB openTSDBClient = OpenTSDBConnFactory.getOpenTSDBClient(source)) {
            return openTSDBClient.suggest(type, metric, prefix, max);
        } catch (IOException e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public String version(ISourceDTO source) {
        try (TSDB openTSDBClient = OpenTSDBConnFactory.getOpenTSDBClient(source)) {
            return openTSDBClient.version();
        } catch (IOException e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public Map<String, String> getVersionInfo(ISourceDTO source) {
        try (TSDB openTSDBClient = OpenTSDBConnFactory.getOpenTSDBClient(source)) {
            return openTSDBClient.getVersionInfo();
        } catch (IOException e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }
}
