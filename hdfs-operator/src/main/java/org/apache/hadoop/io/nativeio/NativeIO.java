package org.apache.hadoop.io.nativeio;

//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//



import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.HardLink;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SecureIOUtils;
import org.apache.hadoop.io.nativeio.Errno;
import org.apache.hadoop.io.nativeio.NativeIOException;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.hadoop.util.PerformanceAdvisory;
import org.apache.hadoop.util.Shell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Cleaner;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

@Private
@Unstable
public class NativeIO {
    private static boolean workaroundNonThreadSafePasswdCalls = false;
    private static final Logger LOG = LoggerFactory.getLogger(org.apache.hadoop.io.nativeio.NativeIO.class);
    private static boolean nativeLoaded = false;
    private static final Map<Long, org.apache.hadoop.io.nativeio.NativeIO.CachedUid> uidCache;
    private static long cacheTimeout;
    private static boolean initialized;

    public NativeIO() {
    }

    public static boolean isAvailable() {
        return NativeCodeLoader.isNativeCodeLoaded() && nativeLoaded;
    }

    private static native void initNative();

    static long getMemlockLimit() {
        return isAvailable() ? getMemlockLimit0() : 0L;
    }

    private static native long getMemlockLimit0();

    static long getOperatingSystemPageSize() {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            Unsafe unsafe = (Unsafe)f.get((Object)null);
            return (long)unsafe.pageSize();
        } catch (Throwable var2) {
            LOG.warn("Unable to get operating system page size.  Guessing 4096.", var2);
            return 4096L;
        }
    }

    private static String stripDomain(String name) {
        int i = name.indexOf(92);
        if (i != -1) {
            name = name.substring(i + 1);
        }

        return name;
    }

    public static String getOwner(FileDescriptor fd) throws IOException {
        ensureInitialized();
        if (Shell.WINDOWS) {
            String owner = org.apache.hadoop.io.nativeio.NativeIO.Windows.getOwner(fd);
            owner = stripDomain(owner);
            return owner;
        } else {
            long uid = org.apache.hadoop.io.nativeio.NativeIO.POSIX.getUIDforFDOwnerforOwner(fd);
            org.apache.hadoop.io.nativeio.NativeIO.CachedUid cUid = (org.apache.hadoop.io.nativeio.NativeIO.CachedUid)uidCache.get(uid);
            long now = System.currentTimeMillis();
            if (cUid != null && cUid.timestamp + cacheTimeout > now) {
                return cUid.username;
            } else {
                String user = org.apache.hadoop.io.nativeio.NativeIO.POSIX.getUserName(uid);
                LOG.info("Got UserName " + user + " for UID " + uid + " from the native implementation");
                cUid = new org.apache.hadoop.io.nativeio.NativeIO.CachedUid(user, now);
                uidCache.put(uid, cUid);
                return user;
            }
        }
    }

    public static FileDescriptor getShareDeleteFileDescriptor(File f, long seekOffset) throws IOException {
        if (!Shell.WINDOWS) {
            RandomAccessFile rf = new RandomAccessFile(f, "r");
            if (seekOffset > 0L) {
                rf.seek(seekOffset);
            }

            return rf.getFD();
        } else {
            FileDescriptor fd = org.apache.hadoop.io.nativeio.NativeIO.Windows.createFile(f.getAbsolutePath(), 2147483648L, 7L, 3L);
            if (seekOffset > 0L) {
                org.apache.hadoop.io.nativeio.NativeIO.Windows.setFilePointer(fd, seekOffset, 0L);
            }

            return fd;
        }
    }

    public static FileOutputStream getCreateForWriteFileOutputStream(File f, int permissions) throws IOException {
        FileDescriptor fd;
        if (!Shell.WINDOWS) {
            try {
                fd = org.apache.hadoop.io.nativeio.NativeIO.POSIX.open(f.getAbsolutePath(), org.apache.hadoop.io.nativeio.NativeIO.POSIX.O_WRONLY | org.apache.hadoop.io.nativeio.NativeIO.POSIX.O_CREAT | org.apache.hadoop.io.nativeio.NativeIO.POSIX.O_EXCL, permissions);
                return new FileOutputStream(fd);
            } catch (NativeIOException var3) {
                if (var3.getErrno() == Errno.EEXIST) {
                    throw new SecureIOUtils.AlreadyExistsException(var3);
                } else {
                    throw var3;
                }
            }
        } else {
            try {
                fd = org.apache.hadoop.io.nativeio.NativeIO.Windows.createFile(f.getCanonicalPath(), 1073741824L, 7L, 1L);
                org.apache.hadoop.io.nativeio.NativeIO.POSIX.chmod(f.getCanonicalPath(), permissions);
                return new FileOutputStream(fd);
            } catch (NativeIOException var4) {
                if (var4.getErrorCode() == 80L) {
                    throw new SecureIOUtils.AlreadyExistsException(var4);
                } else {
                    throw var4;
                }
            }
        }
    }

    private static synchronized void ensureInitialized() {
        if (!initialized) {
            cacheTimeout = (new Configuration()).getLong("hadoop.security.uid.cache.secs", 14400L) * 1000L;
            LOG.info("Initialized cache for UID to User mapping with a cache timeout of " + cacheTimeout / 1000L + " seconds.");
            initialized = true;
        }

    }

    public static void renameTo(File src, File dst) throws IOException {
        if (!nativeLoaded) {
            if (!src.renameTo(dst)) {
                throw new IOException("renameTo(src=" + src + ", dst=" + dst + ") failed.");
            }
        } else {
            renameTo0(src.getAbsolutePath(), dst.getAbsolutePath());
        }

    }

    /** @deprecated */
    @Deprecated
    public static void link(File src, File dst) throws IOException {
        if (!nativeLoaded) {
            HardLink.createHardLink(src, dst);
        } else {
            link0(src.getAbsolutePath(), dst.getAbsolutePath());
        }

    }

    private static native void renameTo0(String var0, String var1) throws NativeIOException;

    private static native void link0(String var0, String var1) throws NativeIOException;

    public static void copyFileUnbuffered(File src, File dst) throws IOException {
        if (nativeLoaded && Shell.WINDOWS) {
            copyFileUnbuffered0(src.getAbsolutePath(), dst.getAbsolutePath());
        } else {
            FileInputStream fis = null;
            FileOutputStream fos = null;
            FileChannel input = null;
            FileChannel output = null;

            try {
                fis = new FileInputStream(src);
                fos = new FileOutputStream(dst);
                input = fis.getChannel();
                output = fos.getChannel();
                long remaining = input.size();
                long position = 0L;

                for(long transferred = 0L; remaining > 0L; position += transferred) {
                    transferred = input.transferTo(position, remaining, output);
                    remaining -= transferred;
                }
            } finally {
                IOUtils.cleanupWithLogger(LOG, new Closeable[]{output});
                IOUtils.cleanupWithLogger(LOG, new Closeable[]{fos});
                IOUtils.cleanupWithLogger(LOG, new Closeable[]{input});
                IOUtils.cleanupWithLogger(LOG, new Closeable[]{fis});
            }
        }

    }

    private static native void copyFileUnbuffered0(String var0, String var1) throws NativeIOException;

    static {
        if (NativeCodeLoader.isNativeCodeLoaded()) {
            try {
                initNative();
                nativeLoaded = true;
            } catch (Throwable var1) {
                PerformanceAdvisory.LOG.debug("Unable to initialize NativeIO libraries", var1);
            }
        }

        uidCache = new ConcurrentHashMap();
        initialized = false;
    }

    private static class CachedUid {
        final long timestamp;
        final String username;

        public CachedUid(String username, long timestamp) {
            this.timestamp = timestamp;
            this.username = username;
        }
    }

    public static class Windows {
        public static final long GENERIC_READ = 2147483648L;
        public static final long GENERIC_WRITE = 1073741824L;
        public static final long FILE_SHARE_READ = 1L;
        public static final long FILE_SHARE_WRITE = 2L;
        public static final long FILE_SHARE_DELETE = 4L;
        public static final long CREATE_NEW = 1L;
        public static final long CREATE_ALWAYS = 2L;
        public static final long OPEN_EXISTING = 3L;
        public static final long OPEN_ALWAYS = 4L;
        public static final long TRUNCATE_EXISTING = 5L;
        public static final long FILE_BEGIN = 0L;
        public static final long FILE_CURRENT = 1L;
        public static final long FILE_END = 2L;
        public static final long FILE_ATTRIBUTE_NORMAL = 128L;

        public Windows() {
        }

        public static void createDirectoryWithMode(File path, int mode) throws IOException {
            createDirectoryWithMode0(path.getAbsolutePath(), mode);
        }

        private static native void createDirectoryWithMode0(String var0, int var1) throws NativeIOException;

        public static native FileDescriptor createFile(String var0, long var1, long var3, long var5) throws IOException;

        public static FileOutputStream createFileOutputStreamWithMode(File path, boolean append, int mode) throws IOException {
            long desiredAccess = 1073741824L;
            long shareMode = 3L;
            long creationDisposition = append ? 4L : 2L;
            return new FileOutputStream(createFileWithMode0(path.getAbsolutePath(), desiredAccess, shareMode, creationDisposition, mode));
        }

        private static native FileDescriptor createFileWithMode0(String var0, long var1, long var3, long var5, int var7) throws NativeIOException;

        public static native long setFilePointer(FileDescriptor var0, long var1, long var3) throws IOException;

        private static native String getOwner(FileDescriptor var0) throws IOException;

        private static native boolean access0(String var0, int var1);

        public static boolean access(String path, org.apache.hadoop.io.nativeio.NativeIO.Windows.AccessRight desiredAccess) throws IOException {
            //return access0(path, desiredAccess.accessRight());
            return true;
        }

        public static native void extendWorkingSetSize(long var0) throws IOException;

        static {
            if (NativeCodeLoader.isNativeCodeLoaded()) {
                try {
                    org.apache.hadoop.io.nativeio.NativeIO.initNative();
                    org.apache.hadoop.io.nativeio.NativeIO.nativeLoaded = true;
                } catch (Throwable var1) {
                    PerformanceAdvisory.LOG.debug("Unable to initialize NativeIO libraries", var1);
                }
            }

        }

        public static enum AccessRight {
            ACCESS_READ(1),
            ACCESS_WRITE(2),
            ACCESS_EXECUTE(32);

            private final int accessRight;

            private AccessRight(int access) {
                this.accessRight = access;
            }

            public int accessRight() {
                return this.accessRight;
            }
        }
    }

    public static class POSIX {
        public static int O_RDONLY = -1;
        public static int O_WRONLY = -1;
        public static int O_RDWR = -1;
        public static int O_CREAT = -1;
        public static int O_EXCL = -1;
        public static int O_NOCTTY = -1;
        public static int O_TRUNC = -1;
        public static int O_APPEND = -1;
        public static int O_NONBLOCK = -1;
        public static int O_SYNC = -1;
        public static int POSIX_FADV_NORMAL = -1;
        public static int POSIX_FADV_RANDOM = -1;
        public static int POSIX_FADV_SEQUENTIAL = -1;
        public static int POSIX_FADV_WILLNEED = -1;
        public static int POSIX_FADV_DONTNEED = -1;
        public static int POSIX_FADV_NOREUSE = -1;
        public static int SYNC_FILE_RANGE_WAIT_BEFORE = 1;
        public static int SYNC_FILE_RANGE_WRITE = 2;
        public static int SYNC_FILE_RANGE_WAIT_AFTER = 4;
        private static final Logger LOG = LoggerFactory.getLogger(org.apache.hadoop.io.nativeio.NativeIO.class);
        public static boolean fadvisePossible = false;
        private static boolean nativeLoaded = false;
        private static boolean syncFileRangePossible = true;
        static final String WORKAROUND_NON_THREADSAFE_CALLS_KEY = "hadoop.workaround.non.threadsafe.getpwuid";
        static final boolean WORKAROUND_NON_THREADSAFE_CALLS_DEFAULT = true;
        private static long cacheTimeout = -1L;
        private static org.apache.hadoop.io.nativeio.NativeIO.POSIX.CacheManipulator cacheManipulator = new org.apache.hadoop.io.nativeio.NativeIO.POSIX.CacheManipulator();
        private static final Map<Integer, org.apache.hadoop.io.nativeio.NativeIO.POSIX.CachedName> USER_ID_NAME_CACHE;
        private static final Map<Integer, org.apache.hadoop.io.nativeio.NativeIO.POSIX.CachedName> GROUP_ID_NAME_CACHE;
        public static final int MMAP_PROT_READ = 1;
        public static final int MMAP_PROT_WRITE = 2;
        public static final int MMAP_PROT_EXEC = 4;

        public POSIX() {
        }

        public static org.apache.hadoop.io.nativeio.NativeIO.POSIX.CacheManipulator getCacheManipulator() {
            return cacheManipulator;
        }

        public static void setCacheManipulator(org.apache.hadoop.io.nativeio.NativeIO.POSIX.CacheManipulator cacheManipulator) {
            org.apache.hadoop.io.nativeio.NativeIO.POSIX.cacheManipulator = cacheManipulator;
        }

        public static boolean isAvailable() {
            return NativeCodeLoader.isNativeCodeLoaded() && nativeLoaded;
        }

        private static void assertCodeLoaded() throws IOException {
            if (!isAvailable()) {
                throw new IOException("NativeIO was not loaded");
            }
        }

        public static native FileDescriptor open(String var0, int var1, int var2) throws IOException;

        private static native org.apache.hadoop.io.nativeio.NativeIO.POSIX.Stat fstat(FileDescriptor var0) throws IOException;

        private static native void chmodImpl(String var0, int var1) throws IOException;

        public static void chmod(String path, int mode) throws IOException {
            if (!Shell.WINDOWS) {
                chmodImpl(path, mode);
            } else {
                try {
                    chmodImpl(path, mode);
                } catch (NativeIOException var3) {
                    if (var3.getErrorCode() == 3L) {
                        throw new NativeIOException("No such file or directory", Errno.ENOENT);
                    }

                    LOG.warn(String.format("NativeIO.chmod error (%d): %s", var3.getErrorCode(), var3.getMessage()));
                    throw new NativeIOException("Unknown error", Errno.UNKNOWN);
                }
            }

        }

        static native void posix_fadvise(FileDescriptor var0, long var1, long var3, int var5) throws NativeIOException;

        static native void sync_file_range(FileDescriptor var0, long var1, long var3, int var5) throws NativeIOException;

        static void posixFadviseIfPossible(String identifier, FileDescriptor fd, long offset, long len, int flags) throws NativeIOException {
            if (nativeLoaded && fadvisePossible) {
                try {
                    posix_fadvise(fd, offset, len, flags);
                } catch (UnsatisfiedLinkError var8) {
                    fadvisePossible = false;
                }
            }

        }

        public static void syncFileRangeIfPossible(FileDescriptor fd, long offset, long nbytes, int flags) throws NativeIOException {
            if (nativeLoaded && syncFileRangePossible) {
                try {
                    sync_file_range(fd, offset, nbytes, flags);
                } catch (UnsupportedOperationException var7) {
                    syncFileRangePossible = false;
                } catch (UnsatisfiedLinkError var8) {
                    syncFileRangePossible = false;
                }
            }

        }

        static native void mlock_native(ByteBuffer var0, long var1) throws NativeIOException;

        static void mlock(ByteBuffer buffer, long len) throws IOException {
            assertCodeLoaded();
            if (!buffer.isDirect()) {
                throw new IOException("Cannot mlock a non-direct ByteBuffer");
            } else {
                mlock_native(buffer, len);
            }
        }

        public static void munmap(MappedByteBuffer buffer) {
            if (buffer instanceof DirectBuffer) {
                Cleaner cleaner = ((DirectBuffer)buffer).cleaner();
                cleaner.clean();
            }

        }

        private static native long getUIDforFDOwnerforOwner(FileDescriptor var0) throws IOException;

        private static native String getUserName(long var0) throws IOException;

        public static org.apache.hadoop.io.nativeio.NativeIO.POSIX.Stat getFstat(FileDescriptor fd) throws IOException {
            org.apache.hadoop.io.nativeio.NativeIO.POSIX.Stat stat = null;
            if (!Shell.WINDOWS) {
                stat = fstat(fd);
                stat.owner = getName(org.apache.hadoop.io.nativeio.NativeIO.POSIX.IdCache.USER, stat.ownerId);
                stat.group = getName(org.apache.hadoop.io.nativeio.NativeIO.POSIX.IdCache.GROUP, stat.groupId);
            } else {
                try {
                    stat = fstat(fd);
                } catch (NativeIOException var3) {
                    if (var3.getErrorCode() == 6L) {
                        throw new NativeIOException("The handle is invalid.", Errno.EBADF);
                    }

                    LOG.warn(String.format("NativeIO.getFstat error (%d): %s", var3.getErrorCode(), var3.getMessage()));
                    throw new NativeIOException("Unknown error", Errno.UNKNOWN);
                }
            }

            return stat;
        }

        private static String getName(org.apache.hadoop.io.nativeio.NativeIO.POSIX.IdCache domain, int id) throws IOException {
            Map<Integer, org.apache.hadoop.io.nativeio.NativeIO.POSIX.CachedName> idNameCache = domain == org.apache.hadoop.io.nativeio.NativeIO.POSIX.IdCache.USER ? USER_ID_NAME_CACHE : GROUP_ID_NAME_CACHE;
            org.apache.hadoop.io.nativeio.NativeIO.POSIX.CachedName cachedName = (org.apache.hadoop.io.nativeio.NativeIO.POSIX.CachedName)idNameCache.get(id);
            long now = System.currentTimeMillis();
            String name;
            if (cachedName != null && cachedName.timestamp + cacheTimeout > now) {
                name = cachedName.name;
            } else {
                name = domain == org.apache.hadoop.io.nativeio.NativeIO.POSIX.IdCache.USER ? getUserName(id) : getGroupName(id);
                if (LOG.isDebugEnabled()) {
                    String type = domain == org.apache.hadoop.io.nativeio.NativeIO.POSIX.IdCache.USER ? "UserName" : "GroupName";
                    LOG.debug("Got " + type + " " + name + " for ID " + id + " from the native implementation");
                }

                cachedName = new org.apache.hadoop.io.nativeio.NativeIO.POSIX.CachedName(name, now);
                idNameCache.put(id, cachedName);
            }

            return name;
        }

        static native String getUserName(int var0) throws IOException;

        static native String getGroupName(int var0) throws IOException;

        public static native long mmap(FileDescriptor var0, int var1, boolean var2, long var3) throws IOException;

        public static native void munmap(long var0, long var2) throws IOException;

        static {
            if (NativeCodeLoader.isNativeCodeLoaded()) {
                try {
                    Configuration conf = new Configuration();
                    org.apache.hadoop.io.nativeio.NativeIO.workaroundNonThreadSafePasswdCalls = conf.getBoolean("hadoop.workaround.non.threadsafe.getpwuid", true);
                    org.apache.hadoop.io.nativeio.NativeIO.initNative();
                    nativeLoaded = true;
                    cacheTimeout = conf.getLong("hadoop.security.uid.cache.secs", 14400L) * 1000L;
                    LOG.debug("Initialized cache for IDs to User/Group mapping with a  cache timeout of " + cacheTimeout / 1000L + " seconds.");
                } catch (Throwable var1) {
                    PerformanceAdvisory.LOG.debug("Unable to initialize NativeIO libraries", var1);
                }
            }

            USER_ID_NAME_CACHE = new ConcurrentHashMap();
            GROUP_ID_NAME_CACHE = new ConcurrentHashMap();
        }

        private static enum IdCache {
            USER,
            GROUP;

            private IdCache() {
            }
        }

        private static class CachedName {
            final long timestamp;
            final String name;

            public CachedName(String name, long timestamp) {
                this.name = name;
                this.timestamp = timestamp;
            }
        }

        public static class Stat {
            private int ownerId;
            private int groupId;
            private String owner;
            private String group;
            private int mode;
            public static int S_IFMT = -1;
            public static int S_IFIFO = -1;
            public static int S_IFCHR = -1;
            public static int S_IFDIR = -1;
            public static int S_IFBLK = -1;
            public static int S_IFREG = -1;
            public static int S_IFLNK = -1;
            public static int S_IFSOCK = -1;
            public static int S_ISUID = -1;
            public static int S_ISGID = -1;
            public static int S_ISVTX = -1;
            public static int S_IRUSR = -1;
            public static int S_IWUSR = -1;
            public static int S_IXUSR = -1;

            Stat(int ownerId, int groupId, int mode) {
                this.ownerId = ownerId;
                this.groupId = groupId;
                this.mode = mode;
            }

            Stat(String owner, String group, int mode) {
                if (!Shell.WINDOWS) {
                    this.owner = owner;
                } else {
                    this.owner = org.apache.hadoop.io.nativeio.NativeIO.stripDomain(owner);
                }

                if (!Shell.WINDOWS) {
                    this.group = group;
                } else {
                    this.group = org.apache.hadoop.io.nativeio.NativeIO.stripDomain(group);
                }

                this.mode = mode;
            }

            public String toString() {
                return "Stat(owner='" + this.owner + "', group='" + this.group + "', mode=" + this.mode + ")";
            }

            public String getOwner() {
                return this.owner;
            }

            public String getGroup() {
                return this.group;
            }

            public int getMode() {
                return this.mode;
            }
        }

        @VisibleForTesting
        public static class NoMlockCacheManipulator extends org.apache.hadoop.io.nativeio.NativeIO.POSIX.CacheManipulator {
            public NoMlockCacheManipulator() {
            }

            public void mlock(String identifier, ByteBuffer buffer, long len) throws IOException {
                org.apache.hadoop.io.nativeio.NativeIO.POSIX.LOG.info("mlocking " + identifier);
            }

            public long getMemlockLimit() {
                return 1125899906842624L;
            }

            public long getOperatingSystemPageSize() {
                return 4096L;
            }

            public boolean verifyCanMlock() {
                return true;
            }
        }

        @VisibleForTesting
        public static class CacheManipulator {
            public CacheManipulator() {
            }

            public void mlock(String identifier, ByteBuffer buffer, long len) throws IOException {
                org.apache.hadoop.io.nativeio.NativeIO.POSIX.mlock(buffer, len);
            }

            public long getMemlockLimit() {
                return org.apache.hadoop.io.nativeio.NativeIO.getMemlockLimit();
            }

            public long getOperatingSystemPageSize() {
                return org.apache.hadoop.io.nativeio.NativeIO.getOperatingSystemPageSize();
            }

            public void posixFadviseIfPossible(String identifier, FileDescriptor fd, long offset, long len, int flags) throws NativeIOException {
                org.apache.hadoop.io.nativeio.NativeIO.POSIX.posixFadviseIfPossible(identifier, fd, offset, len, flags);
            }

            public boolean verifyCanMlock() {
                return org.apache.hadoop.io.nativeio.NativeIO.isAvailable();
            }
        }
    }
}
