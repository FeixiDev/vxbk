# -*- coding:utf-8 -*-
import db
import os
import subprocess
import time
import random
import log
import re
import sys
from prettytable import PrettyTable

db_file = f'/mnt/checksum.json'

logger = log.Log()
db = db.Database(db_file)

def execute_cmd(cmd, timeout=60):
    p = subprocess.Popen(cmd, stderr=subprocess.STDOUT,
                         stdout=subprocess.PIPE, shell=True)
    t_beginning = time.time()
    seconds_passed = 0
    if timeout > 0:
        while True:
            if p.poll() is not None:
                break
            seconds_passed = time.time() - t_beginning
            if timeout and seconds_passed > timeout:
                p.terminate()
                raise TimeoutError(cmd, timeout)
            time.sleep(0.1)
    output = p.stdout.read().decode()
    return output

def findlvdevice(resource):
    cmd = f"/sbin/lvdisplay | grep {resource} | awk '''{{print $3}}''' | awk '''{{print $1}}''' | head -n 1"
    lvdevice = execute_cmd(cmd, 0).strip().split("\n")
    device = lvdevice[len(lvdevice) - 1]
    return device

def createsnapshot(resource, snapshot):
    cmd = f"linstor snapshot create {resource} {snapshot}"
    res = execute_cmd(cmd, 0)
    if "ERROR" in res:
        return False
    else:
        return True

def checkpath(checkarg):
    return os.path.exists(checkarg)

def getchecksum(checkarg):
    if checkpath(checkarg):
        cmd = f"md5sum {checkarg}"
        checksum = execute_cmd(cmd, 0).split(" ")[0]
        return checksum
    else:
        return False

def checkresource(resource):
    cmd = f"linstor r l | grep {resource}"
    res = execute_cmd(cmd, 0)
    if res:
        return True
    else:
        return False

def ddcmd(inp, out):
    cmd = f"dd if={inp} of={out} bs=1M oflag=direct status=progress"
    res = execute_cmd(cmd, 0)
    if "failed" in res:
        return False
    elif "error" in res:
        return False
    elif "No space left on device" in res:
        return False
    else:
        return True

def checkcapcity(path):
    cmd = f"df -h '{path}' | sed -n 2p | awk '''{{print $4}}'''"
    fcap = execute_cmd(cmd, 0).replace('\n', '').replace('\r', '')
    cmd = f"df -h '{path}' | sed -n 2p | awk '''{{print $5}}'''"
    fuse = execute_cmd(cmd, 0).replace('\n', '').replace('\r', '')
    return fcap, fuse

def snapshotrestore(resource, snapshot, restorres):
    cmd = f"linstor resource-definition create {restorres}"
    res = execute_cmd(cmd, 0)
    if "ERROR" in res:
        return False
    cmd = f"linstor snapshot volume-definition restore \
    --from-resource {resource} \
    --from-snapshot {snapshot} \
    --to-resource {restorres}"
    res = execute_cmd(cmd, 0)
    if "ERROR" in res:
        return False
    cmd = f"linstor snapshot resource restore \
    --from-resource {resource} \
    --from-snapshot {snapshot} \
    --to-resource {restorres}"
    res = execute_cmd(cmd, 0)
    if "ERROR" in res:
        return False
    return True

def imagerestore(image, restorres, sp):
    cmd = f"ls -l --block-size=K {image} | awk '''{{print $5}}'''"
    res_size = execute_cmd(cmd, 0).strip()
    cmd = f"linstor resource-definition create {restorres}"
    res = execute_cmd(cmd, 0)
    if "ERROR" in res:
        return False
    cmd = f"linstor volume-definition create {restorres} {res_size}"
    res = execute_cmd(cmd, 0)
    if "ERROR" in res:
        return False
    cmd = f"kubectl get pod -o wide | grep '^backup-' | awk '''{{print $7}}'''"
    node = execute_cmd(cmd, 0).strip()
    if not node:
        return False
    cmd = f"linstor resource create {node} {restorres} --storage-pool {sp}"
    res = execute_cmd(cmd, 0)
    if "ERROR" in res:
        return False
    return True

def createlv(image, restorlv, vg):
    cmd = f"ls -lh {image} | awk '''{{print $5}}'''"
    lv_size = execute_cmd(cmd, 0).strip()
    cmd = f"lvcreate -Zn -n {restorlv} -L {lv_size} {vg}"
    res = execute_cmd(cmd, 0)
    if "created" in res:
        return True
    else:
        return False

def finddrbddevice(resource):
    cmd = f"linstor r lv | grep {resource} | awk '''{{print $12}}'''"
    device = execute_cmd(cmd, 0).strip()
    if not device:
        return False
    return device

def formatdevice(device):
    cmd = f"mkfs.ext4 {device}"
    res = execute_cmd(cmd, 0)
    if "done" not in res:
        return False
    else:
        return True

def show_snap(res_name, snapshot_name):
    data = db.printdb()
    thres = findthinres()
    table = []
    datares = []
    for item in data:
        del item['checksum']
        table.append(item)
    for item in table:
        datares.append(item['name'])
    diff = list(set(thres)^set(datares))
    if diff:
        for item in diff:
            additem = {'name': item, 'snapshot': '', 'snapshotRestore': '', 'image': '', 'imageRestore': '', 'time': ''}
            table.append(additem)
    print(table)

def findthinres():
    cmd = f"linstor sp l | grep True | awk '''{{print $2}}'''"
    thinpools = execute_cmd(cmd, 0).strip().splitlines()
    thinpool = []
    resources = []
    for i in thinpools:
        if i not in thinpool:
            thinpool.append(i)
            cmd = f"linstor r lv | grep {i} | awk '''{{print $4}}'''"
            resources += execute_cmd(cmd, 0).strip().splitlines()
    thresource = []
    for j in resources:
        if j not in thresource:
            thresource.append(j)
    return thresource

def checksnapshot(resource):
    data = db.finddb("name", resource)
    if data:
        lena = len(data)
    else:
        lena = 0
    if lena >= 5:
        deldata = data[0]
        delsp = data[0]["snapshot"]
        logger.write_to_log("INFO", f"资源'{resource}'的快照数量超过5个, 最旧的快照'{delsp}'将会被删除", True)
        result = execute_cmd("linstor snapshot delete " + resource + " " + data[0]["snapshot"])
        if "ERROR" in result:
            logger.write_to_log("ERR", f"删除快照'{delsp}'失败", True)
        if data[0]["image"]:
            delpath = data[0]["image"]
            result = execute_cmd("rm " + data[0]["image"])
            if result:
                logger.write_to_log("ERR", f"删除映像文件'{delpath}'失败", True)
                return False
        res = db.deletedb(data[0]["snapshot"])
        if not res:
            logger.write_to_log("ERR", f"在数据库删除快照'{delsp}'失败", True)
            return False
        logger.write_to_log("INFO", f"成功在数据库删除快照'{delsp}'", True)
        return True
    else:
        return True

def create_snap(res_name):
    if not checkresource(res_name):
        logger.write_to_log("ERR", f"资源'{res_name}'不存在", True)
        print("Failed")
        return 
    if not checksnapshot(res_name):
        logger.write_to_log("ERR", f"为资源'{res_name}'创建快照失败", True)
        print("Failed")
        return

    stime = time.strftime('%Y-%m-%d %H:%M:%S ',time.localtime(time.time()))
    ntime = re.sub(u"([^\u0030-\u0039])", "", stime)
    snap_name = res_name + str(ntime)

    logger.write_to_log("INFO", f"为资源'{res_name}'新建的快照名为：{snap_name}", True)

    res = createsnapshot(res_name, snap_name)
    if not res:
        logger.write_to_log("ERR", f"为资源'{res_name}'创建快照失败", True)
        print("Failed")
        return
    logger.write_to_log("INFO", f"成功为资源'{res_name}'创建快照", True)
    lvdevice = findlvdevice(snap_name)
    if not lvdevice:
        logger.write_to_log("ERR", f"没找到快照'{snap_name}'的设备盘，可能设备盘不在此节点", True)
        print("Failed")
        return
    checksum = getchecksum(lvdevice)
    logger.write_to_log("INFO", f"获取快照设备'{snap_name}'的校验和，并记录在数据库中", True)
    db.insertdb(res_name, snap_name, '', '', '', stime, checksum)
    print("Success")

def checkimage(resource):
    data = db.finddb("name", resource)
    count = 0
    flag = 0
    if data:
        i = len(data)
    else:
        i = 0
    while i > 0:
        if data[i-1]["image"]:
            count = count + 1
            flag = i-1
        i = i - 1
    if count >= 5:
        delpath = data[flag]["image"]
        logger.write_to_log("INFO", f"资源'{resource}'的映像文件数量超过5个, 最旧的映像文件'{delpath}'将会被删除", True)
        result = execute_cmd("rm " + data[flag]["image"])
        if result:
            logger.write_to_log("ERR", f"删除映像文件'{delpath}'失败", True)
            print("Failed")
            return False
        res = db.updatedb("image", "", data[flag]["snapshot"])
        if res:
            logger.write_to_log("ERR", f"在数据库删除映像文件'{delpath}'失败", True)
            print("Failed")
            return False
        logger.write_to_log("INFO", f"成功在数据库删除映像文件'{delpath}'", True)
        return True
    else:
        return True

def dump_snap(res_name, snapshot, backup_dir = "/mnt"):
    if not checkresource(res_name):
        logger.write_to_log("ERR", f"资源'{res_name}不存在'", True)
        print("Failed")
        return 

    if not checkpath(backup_dir):
        logger.write_to_log("ERR", f"路径'{backup_dir}不存在'", True)
        print("Failed")
        return

    if not checkimage(res_name):
        return

    if not os.path.getsize(db_file):
        logger.write_to_log("ERR", f"数据库'{db_file}'不存在", True)
        print("Failed")
        os.remove(db_file)
        return

    backup_record = db.searchdb("snapshot", snapshot)
    snap_name = backup_record["snapshot"]
    snap_md5 = backup_record["checksum"]
    snap_device = findlvdevice(snap_name)
    backup_dir = os.path.abspath(backup_dir)
    if not snap_device:
        logger.write_to_log("ERR", f"无法找到快照'{snapshot}'对应的设备", True)
        print("Failed")
        return
    img_file_name = f"{backup_dir}/{snap_name}.img"

    logger.write_to_log("INFO", f"开始将快照'{snap_name}'导出成映像文件'{img_file_name}'", True)
    res = ddcmd(snap_device,img_file_name)
    if res:
        logger.write_to_log("INFO", f"成功将快照'{snap_name}'导出成映像文件'{img_file_name}'", True)
        logger.write_to_log("INFO", f"开始计算映像文件'{img_file_name}'的校验和", True)

        file_md5 = getchecksum(img_file_name)
        if snap_md5 == file_md5:
            logger.write_to_log("INFO", f"映像文件'{img_file_name}'的校验和和快照的一致，所以保存映像文件", True)
            db.updatedb("image", img_file_name, snap_name)
            fcap, fuse = checkcapcity(backup_dir)
            logger.write_to_log("INFO", f"'{backup_dir}'剩余存储空间为：{fcap}", True)
            if fuse > "80%":
                logger.write_to_log("ALERT", f"备份剩余空间不足20%", True)
            print("Success")
        else:
            logger.write_to_log("ERR", f"映像文件'{img_file_name}'的校验和和快照的不一致，导出失败", True)
            print("Failed")
            return
    else:
        logger.write_to_log("ERR", f"将快照'{snap_name}'导出成映像文件'{img_file_name}'失败", True)
        print("Failed")

def checkchecksum(resource, snapshot):
    logger.write_to_log("INFO", "对比快照设备的校验值和数据库的校验值是否一致", True)
    backup_record = db.selectdb("name", resource, "snapshot", snapshot)
    if not backup_record:
        logger.write_to_log("ERR", f"找不到资源'{resource}'或快照'{snapshot}'", True)
        return
    device = findlvdevice(backup_record["snapshot"])
    md5_dev = getchecksum(device)
    if md5_dev == backup_record["checksum"]:
        logger.write_to_log("INFO", f"快照'{snapshot}'设备的数据未被损坏，可用于还原", True)
        return backup_record
    else:
        return False

def restore_block(resource, snapshot):
    res = checkchecksum(resource, snapshot)
    if res:
        restorres = f"{snapshot}_b"
        res = snapshotrestore(resource, snapshot, restorres)
        if res:
            logger.write_to_log("INFO", f"成功还原资源，资源名为:{snapshot}_b", True)
            db.updatedb("snapshotRestore", restorres, snapshot)
            print("Success")
        else:
            logger.write_to_log("ERR", f"还原资源'{snapshot}_b'失败", True)
            print("Failed")
    else:
        logger.write_to_log("ERR", f"校验和不匹配, 快照设备'{snapshot}'可能被损坏", True)
        print("Failed")

def restore_file(resource, snapshot):
    res = checkchecksum(resource, snapshot)
    if res:
        restorres = f"{snapshot}_b"
        res = snapshotrestore(resource, snapshot, restorres)
        if res:
            logger.write_to_log("INFO", f"成功还原资源，资源名为：{snapshot}_b", True)
            db.updatedb("snapshotRestore", restorres, snapshot)
            cmd = f"mkdir /mnt/{snapshot}_sn"
            res = execute_cmd(cmd, 0)
            if res:
                logger.write_to_log("ERR", f"创建挂载目录'/mnt/{snapshot}_sn'失败", True)
                print("Failed")
                return

            logger.write_to_log("INFO", f"创建挂载目录'/mnt/{snapshot}_sn'成功", True)
            cmd = f"linstor r lv | grep {restorres} | head -n 1 | awk '''{{print $12}}'''"
            sd = execute_cmd(cmd, 0).strip()
            cmd = f"mount {sd} /mnt/{snapshot}_sn"
            res = execute_cmd(cmd, 0)
            if "wrong" in res:
                logger.write_to_log("ERR", f"还原后的设备'{sd}'挂载到目录'/mnt/{snapshot}_sn'失败", True)
                print("Failed")
            else:
                logger.write_to_log("INFO", f"还原后的设备'{sd}'挂载成功, 挂载目录为'/mnt/{snapshot}_sn'", True)
                print("Success")
        else:
            logger.write_to_log("ERR", f"还原资源'{snapshot}_b'失败", True)
            print("Failed")
    else:
        logger.write_to_log("ERR", f"校验和不匹配, 快照设备'{snapshot}'可能被损坏", True)

# def image_restore_block(resource, image, vg):
#     cmd = f"vgs | grep {vg}"
#     res = execute_cmd(cmd, 0)
#     if not res:
#         logger.write_to_log("ERR", f"卷组'{vg}'不存在", True)
#         print("Failed")
#         return
#     logger.write_to_log("INFO", f"比较映像文件'{image}'的校验和和数据库记录的校验和", True)
#     backup_record = db.selectdb("name", resource, "image", image)
#     if not backup_record:
#         logger.write_to_log("ERR", f"资源'{resource}'或者映像文件'{image}'不存在", True)
#         print("Failed")
#         return
#     image_record = getchecksum(image)
#     backup_sp = backup_record["snapshot"]
#     if backup_record["checksum"] == image_record:
#         logger.write_to_log("INFO", f"映像文件'{image}'的校验和和数据库记录的校验和一致", True)
#         logger.write_to_log("INFO", f"开始对资源'{resource}'进行恢复", True)
#         restorlv = f"{backup_sp}_r"
#         res = createlv(image, restorlv, vg)
#         if not res:
#             logger.write_to_log("ERR", f"'{restorlv}'创建失败，映像文件恢复失败", True)
#             print("Failed")
#             return
#         logger.write_to_log("INFO", f"'{restorlv}'创建成功", True)
#         time.sleep(5)
#         device = findlvdevice(restorlv)
#         if not device:
#             logger.write_to_log("ERR", f"找不到'{restorlv}'对应设备，映像文件恢复失败", True)
#             print("Failed")
#             return
#         logger.write_to_log("INFO", f"找到'{restorlv}'对应设备为：{device}", True)
#         res = ddcmd(image, device)
#         if res:
#             logger.write_to_log("INFO", f"资源恢复成功，恢复后的资源名为：{restorlv}", True) 
#             db.updatedb("imageRestore", restorlv, backup_sp)
#             print("Success")
#         else:
#             logger.write_to_log("ERR", f"将映像文件'{image}'导入到'{restorlv}'失败", True)
#             print("Failed")
#     else:
#         logger.write_to_log("ERR", f"校验和不匹配, 映像文件'{image}'可能被损坏", True)
#         print("Failed")


# def image_restore_file(resource, image, vg):
#     cmd = f"vgs | grep {vg}"
#     res = execute_cmd(cmd, 0)
#     if not res:
#         logger.write_to_log("ERR", f"卷组'{vg}'不存在", True)
#         print("Failed")
#         return
#     logger.write_to_log("INFO", f"比较映像文件'{image}'的校验和和数据库记录的校验和", True)
#     backup_record = db.selectdb("name", resource, "image", image)
#     if not backup_record:
#         logger.write_to_log("ERR", f"资源'{resource}'或者映像文件'{image}'不存在", True)
#         print("Failed")
#         return
#     image_record = getchecksum(image)
#     backup_sp = backup_record["snapshot"]
#     if backup_record["checksum"] == image_record:
#         logger.write_to_log("INFO", f"映像文件'{image}'的校验和和数据库记录的校验和一致", True)
#         logger.write_to_log("INFO", f"开始对资源'{resource}'进行恢复", True)
#         restorlv = f"{backup_sp}_r"
#         res = createlv(image, restorlv, vg)
#         if not res:
#             logger.write_to_log("ERR", f"'{restorlv}'创建失败，映像文件恢复失败", True)
#             print("Failed")
#             return
#         logger.write_to_log("INFO", f"'{restorlv}'创建成功", True)
#         time.sleep(5)
#         device = findlvdevice(restorlv)
#         if not device:
#             logger.write_to_log("ERR", f"找不到'{restorlv}'对应设备，映像文件'{image}'恢复失败", True)
#             print("Failed")
#             return
#         logger.write_to_log("INFO", f"找到'{restorlv}'对应设备'{device}'", True)
#         res = formatdevice(device)
#         if not res:
#             logger.write_to_log("ERR", f"格式化设备'{device}'失败", True)
#             logger.write_to_log("ERR", f"资源'{resource}'恢复失败", True)
#             print("Failed")
#             return
#         res = ddcmd(image, device)
#         if res:
#             logger.write_to_log("INFO", f"资源恢复成功，恢复后的资源名为：{restorlv}", True) 
#             db.updatedb("imageRestore", restorlv, backup_sp)
#             cmd = f"mkdir /mnt/{backup_sp}"
#             res = execute_cmd(cmd, 0)
#             if res:
#                 logger.write_to_log("ERR", f"创建挂载目录'/mnt/{backup_sp}'失败", True)
#                 print("Failed")
#                 return
#             logger.write_to_log("INFO", f"创建挂载目录'/mnt/{backup_sp}'成功", True)
#             cmd = f"mount {device} /mnt/{backup_sp}"
#             res = execute_cmd(cmd, 0)
#             if "wrong" in res:
#                 print(f"挂载{device}失败")
#                 logger.write_to_log("ERR", f"还原后的设备'{device}'挂载到目录'/mnt/{backup_sp}'失败", True)
#                 return
#             logger.write_to_log("INFO", f"还原后的设备'{device}'挂载到目录'/mnt/{backup_sp}'成功", True)
#             print("Success")
#         else:
#             logger.write_to_log("ERR", f"将映像文件'{image}'导入到'{restorlv}'失败", True)
#             print("Failed")
#     else:
#         logger.write_to_log("ERR", f"校验和不匹配, 映像文件'{image}'可能被损坏", True)
#         print("Failed")
    
def snapshot_judge(resource, snapshot):
    cmd = f"linstor r lv | grep {resource} | head -n 1 | awk '''{{print $12}}'''"
    sd = execute_cmd(cmd, 0).strip()
    cmd = f"file -s {sd}"
    res = execute_cmd(cmd, 0).strip()
    if "filesystem" in res:
        restore_file(resource, snapshot)
    else:
        restore_block(resource, snapshot)

# def image_judge(resource, image, vg):
#     cmd = f"linstor r lv | grep {resource} | head -n 1 | awk '''{{print $12}}'''"
#     sd = execute_cmd(cmd, 0).strip()
#     cmd = f"file -s {sd}"
#     res = execute_cmd(cmd, 0).strip()
#     if "filesystem" in res:
#         image_restore_file(resource, image, vg)
#     else:
#         image_restore_block(resource, image, vg)


def image_judge(resource, image, sp):
    logger.write_to_log("INFO", f"比较映像文件'{image}'的校验和和数据库记录的校验和", True)
    backup_record = db.selectdb("name", resource, "image", image)
    if not backup_record:
        logger.write_to_log("ERR", f"资源'{resource}'或者映像文件'{image}'不存在", True)
        print("Failed")
        return
    image_record = getchecksum(image)
    backup_sp = backup_record["snapshot"]
    if backup_record["checksum"] == image_record:
        logger.write_to_log("INFO", f"映像文件'{image}'的校验和和数据库记录的校验和一致", True)
        logger.write_to_log("INFO", f"开始对资源'{resource}'进行恢复", True)
        restorres = f"{backup_sp}_r"
        res = imagerestore(image, restorres, sp)
        if not res:
            logger.write_to_log("ERR", f"'{restorres}'创建失败，映像文件恢复失败", True)
            print("Failed")
            return
        logger.write_to_log("INFO", f"'{restorres}'创建成功", True)
        time.sleep(5)
        device = finddrbddevice(restorres)
        if not device:
            logger.write_to_log("ERR", f"找不到'{restorres}'对应设备，映像文件恢复失败", True)
            print("Failed")
            return
        logger.write_to_log("INFO", f"找到'{restorres}'对应设备为：{device}", True)
        res = ddcmd(image, device)
        if res:
            logger.write_to_log("INFO", f"资源恢复成功，恢复后的资源名为：{restorres}", True) 
            db.updatedb("imageRestore", restorres, backup_sp)
            cmd = f"file -s {device}"
            res = execute_cmd(cmd, 0).strip()
            if "filesystem" in res:
                cmd = f"mkdir /mnt/{backup_sp}"
                res = execute_cmd(cmd, 0)
                if res:
                    logger.write_to_log("ERR", f"创建挂载目录'/mnt/{backup_sp}'失败", True)
                    print("Failed")
                    return
                logger.write_to_log("INFO", f"创建挂载目录'/mnt/{backup_sp}'成功", True)
                cmd = f"mount {device} /mnt/{backup_sp}"
                res = execute_cmd(cmd, 0)
                if "wrong" in res:
                    print(f"挂载{device}失败")
                    logger.write_to_log("ERR", f"还原后的设备'{device}'挂载到目录'/mnt/{backup_sp}'失败", True)
                    print("Failed")
                    return
                logger.write_to_log("INFO", f"还原后的设备'{device}'挂载到目录'/mnt/{backup_sp}'成功", True)
            print("Success")
        else:
            logger.write_to_log("ERR", f"将映像文件'{image}'导入到'{restorres}'失败", True)
            print("Failed")
    else:
        logger.write_to_log("ERR", f"校验和不匹配, 映像文件'{image}'可能被损坏", True)
        print("Failed")

