# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Radosgw authication and administration API.
# 
# Runtime depend:
#   python-swift        - Swift client tools
#   radosgw             - REST gateway for RADOS distributed object store
#   python-cloudfiles   - Python language bindings for Cloud Files API
# 
# Chuanyu Tsai 2012/07/05


from swift.common import client as swiftClient
import horizon 
import cloudfiles
import subprocess

# Default ceph user of radosgw 
# Remember add `r' perm to keyring with apache2 user (eg. www-data)
cephuser = 'client.radosgw.gateway'

class RadosGW(object):
    """  """
    def __init__(self, uid, subuser=None, authUrl='http://localhost/auth'):
        self.authUrl, self.uid, self.subuser = authUrl, uid, subuser
        self.accessKey, self.secretKey = None, None
        self._checkRGWInstall()

    def _checkRGWInstall(self):
        return
#       raise Exception('not installed') 

    def _rgwadmin(self, cmd):
        stdRet = subprocess.Popen(args='radosgw-admin -n %s %s' % (cephuser, cmd),
             shell=True,
             stdout=subprocess.PIPE)
        return stdRet.stdout.read()

    def _userInfo(self):
        user = self._rgwadmin('user info --uid="%s"' % self.uid)
        return eval(user)

    def _authSwift(self):
        # radosgw only support auth version 1.0
        return swiftClient.get_auth(self.authUrl, "%s:%s" % (self.uid, self.subuser)
                , self.secretKey.replace('\\',''), auth_version="1.0")

    def _authS3(self):
        """ Not implement yet """
        return

    def _userCreate(self, keyType='swift'):
        user = self._rgwadmin('user create --uid="%s" --display-name="%s" --email="web@site"'
                % (self.uid, self.uid))
        return eval(user)

    def _subuserCreate(self, access='full'):
        # subuser is only for swift api 
        self._rgwadmin('subuser create --subuser="%s:%s"'
                % (self.uid, self.subuser))
        subuser = self._rgwadmin('key create --subuser="%s:%s" --key-type="swift" --access="%s"'
                % (self.uid, self.subuser, access))
        return eval(subuser)

    def authenticate(self, keyType='swift', autoCreate=False):
        """ Return token of radosgw authentication """
        try:
            info = self._userInfo()
        except:
            if autoCreate == False:
                raise Exception('User: %s not found.' % (self.uid))
            else:
                try:
                    info = self._userCreate(keyType) 
                except:
                    raise Exception('User create failed.')

        # Got the user created in rgw,
        # then check the subuser exists or not,
        # if exists, return the token,endpoint 
        if keyType == 'swift':
            for subuser in info['swift_keys']:
                tuser = self.uid + ':' + self.subuser
                if (subuser['user'] == tuser):
                    self.secretKey = subuser['secret_key']
                    return self._authSwift()
        else:   # find S3 access/secret key
            return self._authS3()
        
        if autoCreate == False:
            raise Exception('Subuser: %s:%s not found.' % (self.uid, self.subuser))
        else:
            try:
                self._subuserCreate()
                return self.authenticate(keyType)
            except:
                raise Exception('Subuser create failed.')

    def rmSubuser(self):
        self._rgwadmin('key rm --uid="%s" --subuser="%s:%s" --key-type="swift"'
                % (self.uid, self.uid, self.subuser))
        self._rgwadmin('subuser rm --uid="%s" --subuser="%s:%s"'
                % (self.uid, self.uid, self.subuser))

    def rmUser(self):
        # By http://tracker.newdream.net/issues/2499
        #  & http://tracker.newdream.net/issues/2786
        # rgw now still have to delete objects/buckets manually
        storage_url, auth_token = self.authenticate(autoCreate=True)
        swift_api = cloudfiles.get_connection(
            auth=horizon.api.swift.SwiftAuthentication(storage_url, auth_token) )
        containers = swift_api.get_all_containers()

        for name in containers._names:
            container = swift_api.get_container(name)
            objects = container.get_objects()
            for obj_name in objects._names:
                container.delete_object(obj_name)
            swift_api.delete_container(name)

        self._rgwadmin('user rm --uid="%s"'
                % (self.uid))
