"""Mithril instance provisioning."""

import time
from typing import Any, Dict, List, Optional, Tuple

from sky import sky_logging
from sky.provision import common
from sky.provision.mithril import utils
from sky.provision.mithril.utils import MithrilStatus
from sky.utils import auth_utils
from sky.utils import status_lib

PROVIDER_NAME = 'mithril'

logger = sky_logging.init_logger(__name__)


def _resolve_config(
    provider_config: Optional[Dict[str,
                                   Any]] = None,) -> Optional[Dict[str, str]]:
    """Resolve Mithril API config from stored provider_config.

    If provider_config contains a profile, resolves api_key and api_url
    from that profile in the config file. This ensures status queries use
    the same API server the cluster was launched with, even if the user
    has since switched profiles.

    Returns:
        Resolved config dict, or None to use the current default config.
    """
    if provider_config is None:
        return None
    profile = provider_config.get('profile')
    project_id = provider_config.get('project_id')
    if profile:
        config = utils.get_profile_config(profile)
        if project_id:
            config['project_id'] = project_id
        return config
    # Cluster may have been created via env overrides without a profile.
    return utils.resolve_current_config()


def _reservation_fid(
        provider_config: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """FID of the reservation backing this cluster, if it is reservation-backed.

    Written into the cluster YAML at launch time (see Mithril
    .make_deploy_resources_variables), because a reserved instance is named
    after its reservation and so cannot carry the cluster's name.
    """
    if provider_config is None:
        return None
    return provider_config.get('reservation') or None


def _resume_reservation_if_paused(
    reservation_fid: str,
    resume_stopped_nodes: bool,
    config: Optional[Dict[str, str]] = None,
) -> List[str]:
    """Resume a paused reservation so its instances come back.

    Returns the instance IDs that were resumed, for the ProvisionRecord. A
    reservation whose window has closed is refused rather than touched: the API
    accepts a pause on an expired reservation and then will not resume it, so
    the damage would be irreversible.
    """
    reservation = utils.get_reservation(reservation_fid, config=config)
    if reservation is None:
        raise utils.MithrilError(
            f'Reservation {reservation_fid} not found. It may have been '
            'returned or belong to another project.')

    if utils.reservation_is_finished(reservation):
        raise utils.MithrilError(
            f'Reservation {reservation["name"]} has ended '
            f'(status {reservation.get("status")}, window closed '
            f'{reservation.get("end_time")}). Reserved capacity cannot be '
            'revived; create a new reservation.')

    if reservation.get('status') != 'Paused':
        return []
    if not resume_stopped_nodes:
        raise utils.MithrilError(
            f'Reservation {reservation["name"]} is paused. Run '
            '`ml start <cluster>` to resume it, or '
            f'`ml reservation resume {reservation["name"]}`.')

    logger.debug(f'Resuming paused reservation {reservation_fid}')
    utils.update_reservation(reservation_fid, paused=False, config=config)
    return list(reservation.get('instances') or [])


# SkyPilot re-queries instance statuses immediately after a stop or teardown and
# treats anything other than stopped/terminated as an error, retrying for only
# about ten seconds (_TEARDOWN_WAIT_MAX_ATTEMPTS). A Mithril pause passes through
# STATUS_STOPPING, which maps to ClusterStatus.UP, so returning before the pause
# settles makes `ml stop` / `ml down` fail with "Instances in unexpected state".
_PAUSE_SETTLED_STATUSES: List[MithrilStatus] = [
    'STATUS_PAUSED',
    'STATUS_STOPPED',
    'STATUS_TERMINATED',
]
_PAUSE_WAIT_TIMEOUT_SECONDS = 300
_PAUSE_WAIT_POLL_SECONDS = 5


def _wait_for_reservation_paused(
    reservation_fid: str,
    config: Optional[Dict[str, str]] = None,
) -> None:
    """Wait until a reservation's instances have finished pausing.

    Gives up quietly on timeout rather than raising: the pause has been
    requested and will complete, and failing the teardown would be worse than
    letting SkyPilot's own check report the state it finds.
    """
    deadline = time.time() + _PAUSE_WAIT_TIMEOUT_SECONDS
    while True:
        instances = _filter_instances(
            '',
            status_not_in=_PAUSE_SETTLED_STATUSES,
            config=config,
            reservation_fid=reservation_fid,
        )
        if not instances:
            logger.debug(f'Reservation {reservation_fid}: pause settled.')
            return
        if time.time() >= deadline:
            logger.warning(
                f'Reservation {reservation_fid}: {len(instances)} instance(s) '
                'still pausing after '
                f'{_PAUSE_WAIT_TIMEOUT_SECONDS}s; continuing anyway.')
            return
        logger.debug(f'Reservation {reservation_fid}: waiting for '
                     f'{len(instances)} instance(s) to finish pausing.')
        time.sleep(_PAUSE_WAIT_POLL_SECONDS)


def _filter_instances(
    cluster_name_on_cloud: str,
    status_in: Optional[List[MithrilStatus]] = None,
    status_not_in: Optional[List[MithrilStatus]] = None,
    config: Optional[Dict[str, str]] = None,
    reservation_fid: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    """Filter instances belonging to a cluster, by status.

    Args:
        cluster_name_on_cloud: Cluster name prefix to match. Ignored when
            reservation_fid is given.
        status_in: If provided, only include instances with these statuses.
        status_not_in: If provided, exclude instances with these statuses.
        config: Optional pre-resolved Mithril config for API calls.
        reservation_fid: If given, select the reservation's instances instead of
            matching on name. Bid-created instances are named after the bid, so
            the cluster name is a valid prefix; reserved instances are named
            after their reservation, so only the FID identifies them.
    """
    logger.debug(f'Filtering instances: cluster={cluster_name_on_cloud}, '
                 f'reservation={reservation_fid}, status_in={status_in}, '
                 f'status_not_in={status_not_in}')

    instances = utils.list_instances(config=config)
    filtered_instances: Dict[str, Dict[str, Any]] = {}

    for instance_id, instance in instances.items():
        if reservation_fid is not None:
            if instance.get('reservation') != reservation_fid:
                continue
        elif not instance['name'].startswith(cluster_name_on_cloud):
            continue

        status = instance['status']
        if status_in is not None and status not in status_in:
            continue
        if status_not_in is not None and status in status_not_in:
            continue

        filtered_instances[instance_id] = instance

    logger.debug(f'Found {len(filtered_instances)} instances matching filters')
    return filtered_instances


def run_instances(
    region: str,
    cluster_name: str,
    cluster_name_on_cloud: str,
    config: common.ProvisionConfig,
) -> common.ProvisionRecord:
    """Provision instances for a Mithril cluster.

    Logic:
    1. Check for paused bid and unpause if resume_stopped_nodes is True
    2. Check for existing instances with SSH destinations → use them
    3. Check for existing instances without SSH destinations → wait for them
    4. No instances exist → launch new ones
    """
    logger.debug(f'Starting run_instances with region={region}, '
                 f'cluster={cluster_name_on_cloud}')
    logger.debug(f'Config: {config}')

    # A reservation-backed cluster runs on instances the reservation already
    # created, so there is nothing to bid for: it is adopted, not provisioned.
    reservation_fid = _reservation_fid(config.provider_config)
    resumed_instance_ids: List[str] = []

    if reservation_fid is not None:
        resumed_instance_ids = _resume_reservation_if_paused(
            reservation_fid, resume_stopped_nodes=config.resume_stopped_nodes)
    elif config.resume_stopped_nodes:
        # Check if there's a paused bid that needs to be resumed
        bid = utils.get_bid(cluster_name_on_cloud)
        if bid:
            bid_status = bid.get('status')
            if bid_status == 'Terminated':
                msg = (f'The spot bid ({cluster_name_on_cloud}) for '
                       f'cluster {cluster_name!r} has been terminated on '
                       'Mithril and cannot be resumed. Please use a '
                       'different name.')
                logger.warning(msg)
                raise utils.MithrilError(msg)
            if bid_status == 'Paused':
                bid_id = bid['fid']
                resumed_instance_ids = bid.get('instances', [])
                logger.debug(f'Found paused bid {bid_id}, unpausing')
                utils.update_bid(bid_id, paused=False)

    # Check for existing instances
    all_instances = _filter_instances(
        cluster_name_on_cloud,
        status_not_in=[
            'STATUS_TERMINATED',
        ],
        reservation_fid=reservation_fid,
    )

    # Separate instances with and without SSH destinations
    ready_instances = {}
    pending_instances = {}
    for inst_id, inst in all_instances.items():
        ssh_destination = inst['ssh_destination']
        if ssh_destination:
            ready_instances[inst_id] = inst
        else:
            pending_instances[inst_id] = inst

    logger.debug(f'Found {len(ready_instances)} ready instances, '
                 f'{len(pending_instances)} pending instances')

    # If we have pending instances, wait for them to get SSH destinations
    if pending_instances:
        logger.debug(
            f'Waiting for {len(pending_instances)} pending instances...')
        for instance_id, instance_info in pending_instances.items():
            if not utils.wait_for_ssh_ip(instance_id):
                raise utils.MithrilError(
                    f'Instance {instance_id} failed to get SSH destination')
            ready_instances[instance_id] = instance_info

    # Check if we have enough instances
    desired_count = config.count
    existing_count = len(ready_instances)

    if existing_count >= desired_count:
        # Already have enough instances
        instance_ids = list(ready_instances.keys())
        head_instance_id = instance_ids[0]
        logger.debug(f'Cluster {cluster_name_on_cloud} already has '
                     f'{existing_count} instances')
        return common.ProvisionRecord(
            provider_name=PROVIDER_NAME,
            cluster_name=cluster_name_on_cloud,
            region=region,
            zone=None,
            head_instance_id=head_instance_id,
            resumed_instance_ids=resumed_instance_ids,
            created_instance_ids=[],
        )

    if existing_count > 0 and existing_count < desired_count:
        raise utils.MithrilError(
            f'Cluster {cluster_name_on_cloud} has {existing_count} instances '
            f'but {desired_count} requested. Adding instances to existing '
            f'cluster is not supported.')

    if reservation_fid is not None:
        # Falling through to the bid path here would buy spot capacity on top of
        # a reservation the user is already paying for. A reservation's size is
        # fixed at creation, so the only honest answer is to stop.
        reservation_name = (config.provider_config or
                            {}).get('reservation_name') or reservation_fid
        raise utils.MithrilError(
            f'Reservation {reservation_name} has no usable instances for '
            f'cluster {cluster_name_on_cloud} ({desired_count} requested, '
            f'{existing_count} found). A reservation cannot grow, and its '
            'instances must be authorized with this machine\'s SkyPilot SSH '
            'key when the reservation is created. Check '
            f'`ml reservation info {reservation_name}`.')

    # No instances exist - launch new ones
    to_start_count = desired_count
    logger.debug(f'Launching {to_start_count} new instances')

    instance_type = config.node_config.get('InstanceType')
    if not instance_type:
        raise utils.MithrilError('InstanceType is not set in node_config. '
                                 'Please specify an instance type for Mithril.')

    _, public_key_path = auth_utils.get_or_generate_keys()
    with open(public_key_path, 'r', encoding='utf-8') as f:
        public_key = f.read().strip()

    volume_mounts = config.node_config.get('VolumeMounts', [])
    volume_ids: List[str] = []
    if volume_mounts:
        for volume_mount in volume_mounts:
            volume_fid = volume_mount.get('VolumeIdOnCloud')
            mount_path = volume_mount.get('MountPath')
            if mount_path is None:
                raise utils.MithrilError(
                    'Mithril volume mount path must be set.')
            if not volume_fid:
                raise utils.MithrilError(
                    'Mithril volume fid not found for volume mount.')
            volume_ids.append(volume_fid)

    limit_price = config.node_config.get('LimitPrice')

    bid_id, created_instance_ids = utils.launch_instances(
        instance_type,
        cluster_name_on_cloud,
        region,
        public_key,
        instance_quantity=to_start_count,
        volume_ids=volume_ids,
        limit_price=limit_price,
    )
    logger.debug(
        f'Submitted bid {bid_id}, created {len(created_instance_ids)} instances'
    )

    head_instance_id = created_instance_ids[0]
    return common.ProvisionRecord(
        provider_name=PROVIDER_NAME,
        cluster_name=cluster_name_on_cloud,
        region=region,
        zone=None,
        head_instance_id=head_instance_id,
        resumed_instance_ids=[],
        created_instance_ids=created_instance_ids,
    )


def terminate_instances(
    cluster_name_on_cloud: str,
    provider_config: Optional[dict] = None,
    worker_only: bool = False,
) -> None:
    """Tear down a cluster.

    Bid-backed: cancel the bid (DELETE spot/bids/{bid_id}), which immediately
    terminates its instances.

    Reservation-backed: pause the reservation instead. Reserved capacity is paid
    for up front and cannot be cancelled, so "terminate" cannot mean "stop
    paying". Pausing recovers pause credit for the remainder of the window while
    leaving the reservation intact; returning instances is irreversible and stays
    an explicit `ml reservation return`.
    """
    del worker_only  # unused
    config = _resolve_config(provider_config)
    reservation_fid = _reservation_fid(provider_config)
    logger.debug(
        f'Terminating all instances for cluster {cluster_name_on_cloud}')

    if reservation_fid is not None:
        reservation = utils.get_reservation(reservation_fid, config=config)
        if reservation is None:
            logger.debug(f'Reservation {reservation_fid} no longer exists; '
                         'nothing to release.')
            return
        if utils.reservation_is_finished(reservation):
            # Already over, or already outside its window: pausing an expired
            # reservation is accepted by the API and cannot be undone.
            logger.debug(f'Reservation {reservation["name"]} has ended; '
                         'leaving it untouched.')
            return
        if reservation.get('status') == 'Paused':
            logger.debug(f'Reservation {reservation["name"]} is already '
                         'paused.')
            return
        utils.update_reservation(reservation_fid, paused=True, config=config)
        _wait_for_reservation_paused(reservation_fid, config=config)
        logger.info(
            f'Paused reservation {reservation["name"]} for cluster '
            f'{cluster_name_on_cloud}. The reservation is still yours until '
            f'{reservation.get("end_time")}; paused capacity earns pause '
            f'credit. Use `ml reservation return {reservation["name"]}` to give '
            'the instances back permanently.')
        return

    # Get the bid for this cluster
    bid = utils.get_bid(cluster_name_on_cloud, config=config)
    if not bid:
        logger.debug(f'No bid found for cluster {cluster_name_on_cloud}')
        return

    bid_id = bid['fid']
    utils.cancel_bid(bid_id, config=config)
    logger.debug(f'Canceled bid {bid_id} for cluster {cluster_name_on_cloud}')


def get_cluster_info(
    region: str,
    cluster_name_on_cloud: str,
    provider_config: Optional[Dict[str, Any]] = None,
) -> common.ClusterInfo:
    """Returns information about the cluster.

    Note: We include any instance with an IP address, not just RUNNING
    instances. This allows wait_for_ssh in provisioner.py to handle SSH
    readiness checking, enabling earlier access to instances that have IPs
    but may not be fully RUNNING.
    """
    del region  # unused
    config = _resolve_config(provider_config)
    # Get all non-terminated instances (not just RUNNING) - include any instance
    # with an IP address so that wait_for_ssh can check SSH readiness
    all_instances = _filter_instances(
        cluster_name_on_cloud,
        status_not_in=[
            'STATUS_TERMINATED',
        ],
        config=config,
        reservation_fid=_reservation_fid(provider_config),
    )
    instances: Dict[str, List[common.InstanceInfo]] = {}
    head_instance_id = None
    ssh_user = 'ubuntu'  # Default SSH user for Mithril

    for instance_id, instance_info in all_instances.items():
        # Only include instances with an SSH destination
        ssh_destination = instance_info['ssh_destination']
        if not ssh_destination:
            logger.debug(
                f'Skipping instance {instance_id} - no SSH destination yet')
            continue

        instances[instance_id] = [
            common.InstanceInfo(
                instance_id=instance_id,
                internal_ip=instance_info['private_ip'],
                external_ip=ssh_destination,
                ssh_port=22,
                tags={},
            )
        ]
        if head_instance_id is None:
            head_instance_id = instance_id

    return common.ClusterInfo(
        instances=instances,
        head_instance_id=head_instance_id,
        provider_name=PROVIDER_NAME,
        provider_config=provider_config,
        ssh_user=ssh_user,
    )


def query_instances(
    cluster_name: str,
    cluster_name_on_cloud: str,
    provider_config: Optional[dict] = None,
    non_terminated_only: bool = True,
    retry_if_missing: bool = False,
) -> Dict[str, Tuple[Optional['status_lib.ClusterStatus'], Optional[str]]]:
    """Returns the status of the specified instances for Mithril."""
    del cluster_name, retry_if_missing  # unused
    config = _resolve_config(provider_config)
    instances = _filter_instances(
        cluster_name_on_cloud,
        config=config,
        reservation_fid=_reservation_fid(provider_config),
    )

    statuses: Dict[str, Tuple[Optional['status_lib.ClusterStatus'],
                              Optional[str]]] = {}
    for instance_id, instance in instances.items():
        cluster_status = utils.to_cluster_status(instance['status'])
        if non_terminated_only and cluster_status is None:
            continue
        statuses[instance_id] = (cluster_status, None)
    return statuses


def wait_instances(region: str, cluster_name_on_cloud: str,
                   state: Optional[status_lib.ClusterStatus]) -> None:
    """Wait for instances to reach the desired state.

    For Mithril, waiting is done in run_instances() via wait_for_bid() and
    wait_for_ssh_ip(), so this function is a no-op.
    """
    del region, cluster_name_on_cloud, state  # unused


def stop_instances(
    cluster_name_on_cloud: str,
    provider_config: Optional[Dict[str, Any]] = None,
    worker_only: bool = False,
) -> None:
    """Stop running instances by pausing.

    Bid-backed: pause the bid. Reservation-backed: pause the reservation, which
    hands the capacity back for the rest of the window and earns pause credit.
    Either way `ml start` resumes it.
    """
    del worker_only  # unused
    config = _resolve_config(provider_config)
    reservation_fid = _reservation_fid(provider_config)
    logger.debug(f'Stopping instances for cluster {cluster_name_on_cloud}')

    if reservation_fid is not None:
        reservation = utils.get_reservation(reservation_fid, config=config)
        if reservation is None:
            logger.debug(f'Reservation {reservation_fid} no longer exists.')
            return
        if utils.reservation_is_finished(reservation):
            # Pausing past the window is irreversible; see design 3.6.
            logger.debug(f'Reservation {reservation["name"]} has ended; not '
                         'pausing.')
            return
        if reservation.get('status') == 'Paused':
            logger.debug(f'Reservation {reservation["name"]} already paused.')
            return
        utils.update_reservation(reservation_fid, paused=True, config=config)
        _wait_for_reservation_paused(reservation_fid, config=config)
        logger.debug(f'Paused reservation {reservation_fid} for cluster '
                     f'{cluster_name_on_cloud}')
        return

    bid = utils.get_bid(cluster_name_on_cloud, config=config)
    if not bid:
        logger.debug(f'No bid found for cluster {cluster_name_on_cloud}')
        return

    bid_id = bid['fid']
    utils.update_bid(bid_id, paused=True, config=config)
    logger.debug(f'Paused bid {bid_id} for cluster {cluster_name_on_cloud}')


def cleanup_ports(
    cluster_name_on_cloud: str,
    provider_config: Optional[dict] = None,
    ports: Optional[list] = None,
) -> None:
    """Cleanup ports. Not supported for Mithril."""
    raise NotImplementedError('cleanup_ports is not supported for Mithril')


def cleanup_custom_multi_network(
    cluster_name_on_cloud: str,
    provider_config: Dict[str, Any],
    failover: bool = False,
) -> None:
    """Cleanup custom multi-network. Not supported for Mithril."""
    raise NotImplementedError(
        'cleanup_custom_multi_network is not supported for Mithril')


def open_ports(
    cluster_name_on_cloud: str,
    ports: list,
    provider_config: Optional[dict] = None,
) -> None:
    """Open ports. Not supported for Mithril."""
    raise NotImplementedError('open_ports is not supported for Mithril')
