/*
 * Copyright (c) 2012-2016 by Zalo Group.
 * All Rights Reserved.
 */
package com.vng.zing.userservice.thrift.wrapper;

import com.vng.zing.common.ZCommonDef;
import com.vng.zing.common.ZErrorDef;
import com.vng.zing.logger.ZLogger;
import com.vng.zing.thriftpool.TClientPool;
import com.vng.zing.thriftpool.ZClientPoolUtil;
import com.vng.zing.userservice.thrift.CreateUserParams;
import com.vng.zing.userservice.thrift.CreateUserResult;
import com.vng.zing.userservice.thrift.DeleteUserParams;
import com.vng.zing.userservice.thrift.DeleteUserResult;
import com.vng.zing.userservice.thrift.DetailUserParams;
import com.vng.zing.userservice.thrift.DetailUserResult;
import com.vng.zing.userservice.thrift.ListUserParams;
import com.vng.zing.userservice.thrift.ListUserResult;
import com.vng.zing.userservice.thrift.UpdateUserParams;
import com.vng.zing.userservice.thrift.UpdateUserResult;
import com.vng.zing.userservice.thrift.UserService;
import com.vng.zing.zcommon.thrift.PutPolicy;
import java.nio.ByteBuffer;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

/**
 *
 * @author namnq
 */
public class UserMwClient implements UserService.Iface {

    private static final Class _ThisClass = UserMwClient.class;
    private static final Logger _Logger = ZLogger.getLogger(_ThisClass);
    private final String _name;
    private TClientPool.BizzConfig _bizzCfg;

    public UserMwClient(String name) {
        assert (name != null && !name.isEmpty());
        _name = name;
        _initialize();

    }

    public String getName() {
        return _name;
    }

    private void _initialize() {
        ZClientPoolUtil.SetDefaultPoolProp(_ThisClass //clazzOfCfg
                ,
                 _name //instName
                ,
                 null //host
                ,
                 null //auth
                ,
                 ZCommonDef.TClientTimeoutMilisecsDefault //timeout
                ,
                 ZCommonDef.TClientNRetriesDefault //nretryi
                ,
                 ZCommonDef.TClientMaxRdAtimeDefault //maxRdAtime
                ,
                 ZCommonDef.TClientMaxWrAtimeDefault //maxWrAtimei
        );
        ZClientPoolUtil.GetListPools(_ThisClass, _name, new UserService.Client.Factory()); //auto create pools
        _bizzCfg = ZClientPoolUtil.GetBizzCfg(_ThisClass, _name);
    }

    private TClientPool<UserService.Client> getClientPool() {
        return (TClientPool<UserService.Client>) ZClientPoolUtil.GetPool(_ThisClass, _name);
    }

    private TClientPool.BizzConfig getBizzCfg() {
        return _bizzCfg;
    }

    @Override
    public ListUserResult getUsers(ListUserParams params) throws TException {
        TClientPool.BizzConfig bCfg = getBizzCfg();
        for (int retry = 0; retry < bCfg.getNRetry(); ++retry) {
            TClientPool<UserService.Client> pool = getClientPool();
            UserService.Client cli = pool.borrowClient();
            if (cli == null) {
                return new ListUserResult(ZErrorDef.NO_CONNECTION, null);
            }
            try {
                ListUserResult ret = cli.getUsers(params);
                pool.returnClient(cli);
                return ret;
            } catch (TTransportException ex) {
                pool.invalidateClient(cli, ex);
                _Logger.error(ex);
            } catch (TException ex) {
                pool.invalidateClient(cli, ex);
                _Logger.error(ex);
                return new ListUserResult(ZErrorDef.BAD_REQUEST, null);
            } catch (Exception ex) {
                pool.invalidateClient(cli, ex);
                _Logger.error(ex);
                return new ListUserResult(ZErrorDef.BAD_REQUEST, null);
            }
        }
        return new ListUserResult(ZErrorDef.BAD_CONNECTION, null);
    }

    @Override
    public DetailUserResult getUser(DetailUserParams params) throws TException {
        TClientPool.BizzConfig bCfg = getBizzCfg();
        for (int retry = 0; retry < bCfg.getNRetry(); ++retry) {
            TClientPool<UserService.Client> pool = getClientPool();
            UserService.Client cli = pool.borrowClient();
            if (cli == null) {
                return new DetailUserResult(ZErrorDef.NO_CONNECTION, null);
            }
            try {
                DetailUserResult ret = cli.getUser(params);
                pool.returnClient(cli);
                return ret;
            } catch (TTransportException ex) {
                pool.invalidateClient(cli, ex);
                _Logger.error(ex);
            } catch (TException ex) {
                pool.invalidateClient(cli, ex);
                _Logger.error(ex);
                return new DetailUserResult(ZErrorDef.BAD_REQUEST, null);
            } catch (Exception ex) {
                pool.invalidateClient(cli, ex);
                _Logger.error(ex);
                return new DetailUserResult(ZErrorDef.BAD_REQUEST, null);
            }
        }
        return new DetailUserResult(ZErrorDef.BAD_CONNECTION, null);
    }

    @Override
    public CreateUserResult createUser(CreateUserParams params) throws TException {
        TClientPool.BizzConfig bCfg = getBizzCfg();
        for (int retry = 0; retry < bCfg.getNRetry(); ++retry) {
            TClientPool<UserService.Client> pool = getClientPool();
            UserService.Client cli = pool.borrowClient();
            if (cli == null) {
                return new CreateUserResult(ZErrorDef.NO_CONNECTION, null);
            }
            try {
                CreateUserResult ret = cli.createUser(params);
                pool.returnClient(cli);
                return ret;
            } catch (TTransportException ex) {
                pool.invalidateClient(cli, ex);
                _Logger.error(ex);
            } catch (TException ex) {
                pool.invalidateClient(cli, ex);
                _Logger.error(ex);
                return new CreateUserResult(ZErrorDef.BAD_REQUEST, null);
            } catch (Exception ex) {
                pool.invalidateClient(cli, ex);
                _Logger.error(ex);
                return new CreateUserResult(ZErrorDef.BAD_REQUEST, null);
            }
        }
        return new CreateUserResult(ZErrorDef.BAD_CONNECTION, null);
    }

    @Override
    public UpdateUserResult updateUser(UpdateUserParams params) throws TException {
        TClientPool.BizzConfig bCfg = getBizzCfg();
        for (int retry = 0; retry < bCfg.getNRetry(); ++retry) {
            TClientPool<UserService.Client> pool = getClientPool();
            UserService.Client cli = pool.borrowClient();
            if (cli == null) {
                return new UpdateUserResult(ZErrorDef.NO_CONNECTION, null);
            }
            try {
                UpdateUserResult ret = cli.updateUser(params);
                pool.returnClient(cli);
                return ret;
            } catch (TTransportException ex) {
                pool.invalidateClient(cli, ex);
                _Logger.error(ex);
            } catch (TException ex) {
                pool.invalidateClient(cli, ex);
                _Logger.error(ex);
                return new UpdateUserResult(ZErrorDef.BAD_REQUEST, null);
            } catch (Exception ex) {
                pool.invalidateClient(cli, ex);
                _Logger.error(ex);
                return new UpdateUserResult(ZErrorDef.BAD_REQUEST, null);
            }
        }
        return new UpdateUserResult(ZErrorDef.BAD_CONNECTION, null);
    }

    @Override
    public DeleteUserResult deleteUser(DeleteUserParams params) throws TException {
        TClientPool.BizzConfig bCfg = getBizzCfg();
        for (int retry = 0; retry < bCfg.getNRetry(); ++retry) {
            TClientPool<UserService.Client> pool = getClientPool();
            UserService.Client cli = pool.borrowClient();
            if (cli == null) {
                return new DeleteUserResult(ZErrorDef.NO_CONNECTION, null);
            }
            try {
                DeleteUserResult ret = cli.deleteUser(params);
                pool.returnClient(cli);
                return ret;
            } catch (TTransportException ex) {
                pool.invalidateClient(cli, ex);
                _Logger.error(ex);
            } catch (TException ex) {
                pool.invalidateClient(cli, ex);
                _Logger.error(ex);
                return new DeleteUserResult(ZErrorDef.BAD_REQUEST, null);
            } catch (Exception ex) {
                pool.invalidateClient(cli, ex);
                _Logger.error(ex);
                return new DeleteUserResult(ZErrorDef.BAD_REQUEST, null);
            }
        }
        return new DeleteUserResult(ZErrorDef.BAD_CONNECTION, null);
    }
}
