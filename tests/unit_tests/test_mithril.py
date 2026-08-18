"""Tests for Mithril cloud provider."""

import os

import pytest

from sky import clouds
from sky.clouds import mithril
from sky.provision.mithril import instance as mithril_instance
from sky.provision.mithril import utils as mithril_utils


class TestMithrilCredentialsPath:
    """Test cases for get_credentials_path method."""

    def test_default_path_without_xdg(self, monkeypatch):
        """Test default path when XDG_CONFIG_HOME is not set."""
        monkeypatch.delenv('XDG_CONFIG_HOME', raising=False)

        path = mithril.Mithril.get_credentials_path()

        assert path == '~/.config/mithril/config.yaml'

    def test_path_with_xdg_config_home(self, monkeypatch, tmp_path):
        """Test path respects XDG_CONFIG_HOME when set."""
        xdg_dir = tmp_path / 'custom_config'
        monkeypatch.setenv('XDG_CONFIG_HOME', str(xdg_dir))

        path = mithril.Mithril.get_credentials_path()

        expected = os.path.join(str(xdg_dir), 'mithril', 'config.yaml')
        assert path == expected


class TestMithrilCredentials:
    """Test cases for Mithril credential handling."""

    def test_check_credentials_missing(self, monkeypatch, tmp_path):
        """Test that missing credentials file returns invalid."""
        fake_path = tmp_path / 'config.yaml'
        monkeypatch.setattr(mithril.Mithril, 'get_credentials_path',
                            classmethod(lambda cls: str(fake_path)))
        monkeypatch.delenv('MITHRIL_API_KEY', raising=False)
        monkeypatch.delenv('MITHRIL_PROJECT', raising=False)

        valid, msg = mithril.Mithril.check_credentials(
            clouds.CloudCapability.COMPUTE)

        assert not valid
        assert 'Mithril credentials not found' in msg

    def test_check_credentials_from_file(self, monkeypatch, tmp_path):
        """Test that credentials are valid when config file exists."""
        cred_path = tmp_path / 'config.yaml'
        cred_path.write_text('api_key: test-key')
        monkeypatch.setattr(mithril.Mithril, 'get_credentials_path',
                            classmethod(lambda cls: str(cred_path)))
        monkeypatch.delenv('MITHRIL_API_KEY', raising=False)
        monkeypatch.delenv('MITHRIL_PROJECT', raising=False)

        valid, msg = mithril.Mithril.check_credentials(
            clouds.CloudCapability.COMPUTE)

        assert valid
        assert msg is None

    def test_check_credentials_from_env_vars(self, monkeypatch, tmp_path):
        """Test that credentials are valid when env vars are set."""
        # Ensure no config file exists
        fake_path = tmp_path / 'config.yaml'
        monkeypatch.setattr(mithril.Mithril, 'get_credentials_path',
                            classmethod(lambda cls: str(fake_path)))
        # Set environment variables
        monkeypatch.setenv('MITHRIL_API_KEY', 'test-api-key')
        monkeypatch.setenv('MITHRIL_PROJECT', 'test-project')

        valid, msg = mithril.Mithril.check_credentials(
            clouds.CloudCapability.COMPUTE)

        assert valid
        assert msg is None

    def test_check_credentials_partial_env_vars_invalid(self, monkeypatch,
                                                        tmp_path):
        """Test that only one env var set is not sufficient."""
        fake_path = tmp_path / 'config.yaml'
        monkeypatch.setattr(mithril.Mithril, 'get_credentials_path',
                            classmethod(lambda cls: str(fake_path)))
        # Set only API key, not project
        monkeypatch.setenv('MITHRIL_API_KEY', 'test-api-key')
        monkeypatch.delenv('MITHRIL_PROJECT', raising=False)

        valid, msg = mithril.Mithril.check_credentials(
            clouds.CloudCapability.COMPUTE)

        assert not valid
        assert 'Mithril credentials not found' in msg

    def test_credential_file_mounts_when_file_exists(self, monkeypatch,
                                                     tmp_path):
        """Test get_credential_file_mounts returns correct mapping."""
        # Create the credential file in tmp_path simulating ~/.config/mithril/
        cred_file = tmp_path / '.config' / 'mithril' / 'config.yaml'
        cred_file.parent.mkdir(parents=True)
        cred_file.touch()

        # Use a path with ~ that will be expanded
        unexpanded_path = '~/.config/mithril/config.yaml'
        monkeypatch.setenv('HOME', str(tmp_path))
        monkeypatch.setattr(mithril.Mithril, 'get_credentials_path',
                            classmethod(lambda cls: unexpanded_path))

        mounts = mithril.Mithril.get_credential_file_mounts()

        # The method returns {remote_path: local_path}
        # Key should be the unexpanded remote path, value should be
        # the local expanded path
        expected_expanded = str(cred_file)
        assert unexpanded_path in mounts
        assert mounts[unexpanded_path] == expected_expanded

    def test_credential_file_mounts_when_file_missing(self, monkeypatch,
                                                      tmp_path):
        """Test get_credential_file_mounts returns empty dict when no file."""
        fake_path = tmp_path / 'config.yaml'
        monkeypatch.setattr(mithril.Mithril, 'get_credentials_path',
                            classmethod(lambda cls: str(fake_path)))

        mounts = mithril.Mithril.get_credential_file_mounts()

        assert not mounts


class TestMithrilValidation:
    """Test cases for Mithril validation logic."""

    def test_region_zone_validation_disallows_zones(self):
        """Test that Mithril raises ValueError when zone is specified."""
        cloud = mithril.Mithril()
        with pytest.raises(ValueError, match='does not support zones'):
            cloud.validate_region_zone('some-region', 'zone-1')


class TestGetConfig:
    """Test cases for get_config function with profile-based schema."""

    def test_config_from_profile_file(self, monkeypatch, tmp_path):
        """Test config is loaded from profile-based config file."""
        monkeypatch.delenv('MITHRIL_API_KEY', raising=False)
        monkeypatch.delenv('MITHRIL_PROJECT', raising=False)
        monkeypatch.delenv('MITHRIL_API_URL', raising=False)
        monkeypatch.delenv('MITHRIL_PROFILE', raising=False)

        config_content = """\
current_profile: default
profiles:
  default:
    api_key: file-api-key
    project_id: file-project-id
    api_url: https://custom.api.mithril.ai
"""
        cred_path = tmp_path / 'config.yaml'
        cred_path.write_text(config_content)

        monkeypatch.setattr(mithril_utils, 'get_credentials_path',
                            lambda: str(cred_path))

        config = mithril_utils.resolve_current_config()

        assert config['api_key'] == 'file-api-key'
        assert config['project_id'] == 'file-project-id'
        assert config['api_url'] == 'https://custom.api.mithril.ai'

    def test_env_vars_override_profile(self, monkeypatch, tmp_path):
        """Test environment variables override profile config."""
        config_content = """\
current_profile: default
profiles:
  default:
    api_key: file-api-key
    project_id: file-project-id
    api_url: https://file.api.mithril.ai
"""
        cred_path = tmp_path / 'config.yaml'
        cred_path.write_text(config_content)

        monkeypatch.setattr(mithril_utils, 'get_credentials_path',
                            lambda: str(cred_path))

        # Set env vars to override
        monkeypatch.setenv('MITHRIL_API_KEY', 'env-api-key')
        monkeypatch.setenv('MITHRIL_PROJECT', 'env-project-id')
        monkeypatch.setenv('MITHRIL_API_URL', 'https://env.api.mithril.ai')
        monkeypatch.delenv('MITHRIL_PROFILE', raising=False)

        config = mithril_utils.resolve_current_config()

        assert config['api_key'] == 'env-api-key'
        assert config['project_id'] == 'env-project-id'
        assert config['api_url'] == 'https://env.api.mithril.ai'

    def test_partial_env_vars_with_profile(self, monkeypatch, tmp_path):
        """Test partial env vars combine with profile config."""
        config_content = """\
current_profile: default
profiles:
  default:
    api_key: file-api-key
    project_id: file-project-id
"""
        cred_path = tmp_path / 'config.yaml'
        cred_path.write_text(config_content)

        monkeypatch.setattr(mithril_utils, 'get_credentials_path',
                            lambda: str(cred_path))

        # Only override API key, keep project_id from file
        monkeypatch.setenv('MITHRIL_API_KEY', 'env-api-key')
        monkeypatch.delenv('MITHRIL_PROJECT', raising=False)
        monkeypatch.delenv('MITHRIL_API_URL', raising=False)
        monkeypatch.delenv('MITHRIL_PROFILE', raising=False)

        config = mithril_utils.resolve_current_config()

        assert config['api_key'] == 'env-api-key'
        assert config['project_id'] == 'file-project-id'
        # Default API URL when not specified
        assert config['api_url'] == 'https://api.mithril.ai'

    def test_mithril_profile_env_selects_profile(self, monkeypatch, tmp_path):
        """Test MITHRIL_PROFILE env var selects different profile."""
        config_content = """\
current_profile: default
profiles:
  default:
    api_key: default-key
    project_id: default-proj
  staging:
    api_key: staging-key
    project_id: staging-proj
"""
        cred_path = tmp_path / 'config.yaml'
        cred_path.write_text(config_content)

        monkeypatch.setattr(mithril_utils, 'get_credentials_path',
                            lambda: str(cred_path))

        monkeypatch.delenv('MITHRIL_API_KEY', raising=False)
        monkeypatch.delenv('MITHRIL_PROJECT', raising=False)
        monkeypatch.delenv('MITHRIL_API_URL', raising=False)
        monkeypatch.setenv('MITHRIL_PROFILE', 'staging')

        config = mithril_utils.resolve_current_config()

        assert config['api_key'] == 'staging-key'
        assert config['project_id'] == 'staging-proj'

    def test_missing_api_key_raises_error(self, monkeypatch, tmp_path):
        """Test error is raised when API key is not found."""
        config_content = """\
current_profile: default
profiles:
  default:
    project_id: file-project-id
"""
        cred_path = tmp_path / 'config.yaml'
        cred_path.write_text(config_content)

        monkeypatch.setattr(mithril_utils, 'get_credentials_path',
                            lambda: str(cred_path))
        monkeypatch.delenv('MITHRIL_API_KEY', raising=False)
        monkeypatch.delenv('MITHRIL_PROJECT', raising=False)
        monkeypatch.delenv('MITHRIL_PROFILE', raising=False)

        with pytest.raises(mithril_utils.MithrilError,
                           match='API key not found'):
            mithril_utils.resolve_current_config()

    def test_missing_project_id_raises_error(self, monkeypatch, tmp_path):
        """Test error is raised when project ID is not found."""
        config_content = """\
current_profile: default
profiles:
  default:
    api_key: file-api-key
"""
        cred_path = tmp_path / 'config.yaml'
        cred_path.write_text(config_content)

        monkeypatch.setattr(mithril_utils, 'get_credentials_path',
                            lambda: str(cred_path))
        monkeypatch.delenv('MITHRIL_API_KEY', raising=False)
        monkeypatch.delenv('MITHRIL_PROJECT', raising=False)
        monkeypatch.delenv('MITHRIL_PROFILE', raising=False)

        with pytest.raises(mithril_utils.MithrilError,
                           match='project ID not found'):
            mithril_utils.resolve_current_config()

    def test_config_only_from_env_vars_no_file(self, monkeypatch, tmp_path):
        """Test config works with only env vars when no config file exists."""
        fake_path = tmp_path / 'nonexistent.yaml'
        monkeypatch.setattr(mithril_utils, 'get_credentials_path',
                            lambda: str(fake_path))

        monkeypatch.setenv('MITHRIL_API_KEY', 'env-api-key')
        monkeypatch.setenv('MITHRIL_PROJECT', 'env-project-id')
        monkeypatch.delenv('MITHRIL_API_URL', raising=False)
        monkeypatch.delenv('MITHRIL_PROFILE', raising=False)

        config = mithril_utils.resolve_current_config()

        assert config['api_key'] == 'env-api-key'
        assert config['project_id'] == 'env-project-id'
        assert config['api_url'] == 'https://api.mithril.ai'

    def test_missing_current_profile_no_env_raises_error(
            self, monkeypatch, tmp_path):
        """Test error when current_profile is missing and no env vars set.

        Without current_profile or MITHRIL_PROFILE, no profile config is
        loaded.  With no env vars either, _build_config raises because the
        API key is missing.
        """
        monkeypatch.delenv('MITHRIL_API_KEY', raising=False)
        monkeypatch.delenv('MITHRIL_PROJECT', raising=False)
        monkeypatch.delenv('MITHRIL_PROFILE', raising=False)

        config_content = """\
profiles:
  default:
    api_key: key
    project_id: proj
"""
        cred_path = tmp_path / 'config.yaml'
        cred_path.write_text(config_content)

        monkeypatch.setattr(mithril_utils, 'get_credentials_path',
                            lambda: str(cred_path))

        with pytest.raises(mithril_utils.MithrilError,
                           match='API key not found'):
            mithril_utils.resolve_current_config()

    def test_profile_not_found_raises_error(self, monkeypatch, tmp_path):
        """Test error is raised when specified profile doesn't exist."""
        monkeypatch.delenv('MITHRIL_API_KEY', raising=False)
        monkeypatch.delenv('MITHRIL_PROJECT', raising=False)
        monkeypatch.delenv('MITHRIL_PROFILE', raising=False)

        config_content = """\
current_profile: nonexistent
profiles:
  default:
    api_key: key
    project_id: proj
"""
        cred_path = tmp_path / 'config.yaml'
        cred_path.write_text(config_content)

        monkeypatch.setattr(mithril_utils, 'get_credentials_path',
                            lambda: str(cred_path))

        with pytest.raises(mithril_utils.MithrilError,
                           match='profile \'nonexistent\' not found'):
            mithril_utils.resolve_current_config()


class TestMithrilReservations:
    """Test cases for reservation discovery."""

    # A reservation record as returned by GET /v2/reservation, trimmed to the
    # fields the discovery path reads.
    RESERVATION = {
        'fid': 'res_abc',
        'name': 'umang-jordan',
        'status': 'Active',
        'region': 'us-central3-a',
        'instance_type': 'it_a100_central3',
        'instance_quantity': 4,
        'end_time': '2099-01-01T00:00:00Z',
    }

    INSTANCE_TYPES = {
        # Same name, one FID per region.
        'it_a100_central3': {
            'fid': 'it_a100_central3',
            'name': 'a100-80gb.sxm.4x',
        },
        'it_a100_other': {
            'fid': 'it_a100_other',
            'name': 'a100-80gb.sxm.4x',
        },
        'it_h100': {
            'fid': 'it_h100',
            'name': 'h100-80gb.sxm.8x',
        },
    }

    def _patch(self, monkeypatch, reservations):
        monkeypatch.setattr(mithril_utils,
                            'list_reservations',
                            lambda config=None: reservations)
        monkeypatch.setattr(mithril_utils, 'get_instance_types',
                            lambda: self.INSTANCE_TYPES)

    def _available(self, monkeypatch, reservations, **kwargs):
        self._patch(monkeypatch, reservations)
        kwargs.setdefault('instance_type_name', 'a100-80gb.sxm.4x')
        kwargs.setdefault('region', 'us-central3-a')
        return mithril_utils.get_available_reservations(**kwargs)

    def test_reports_whole_reservation(self, monkeypatch):
        assert self._available(monkeypatch, [self.RESERVATION]) == {
            'umang-jordan': 4
        }

    @pytest.mark.parametrize('status', ['Ended', 'Canceled'])
    def test_skips_terminal_statuses(self, monkeypatch, status):
        reservation = {**self.RESERVATION, 'status': status}
        assert self._available(monkeypatch, [reservation]) == {}

    @pytest.mark.parametrize('status', ['Active', 'Pending', 'Paused'])
    def test_includes_usable_statuses(self, monkeypatch, status):
        # Paused counts: resuming is a single PATCH, and pausing is how idle
        # reserved capacity earns credit.
        reservation = {**self.RESERVATION, 'status': status}
        assert self._available(monkeypatch, [reservation]) == {
            'umang-jordan': 4
        }

    def test_skips_expired_window(self, monkeypatch):
        # A reservation can sit in Active past its end time.
        reservation = {**self.RESERVATION, 'end_time': '2020-01-01T00:00:00Z'}
        assert self._available(monkeypatch, [reservation]) == {}

    @pytest.mark.parametrize('end_time', [None, '', 'not-a-timestamp'])
    def test_unparseable_end_time_is_treated_as_expired(self, monkeypatch,
                                                        end_time):
        reservation = {**self.RESERVATION, 'end_time': end_time}
        assert self._available(monkeypatch, [reservation]) == {}

    def test_skips_other_regions(self, monkeypatch):
        reservation = {**self.RESERVATION, 'region': 'us-east1-a'}
        assert self._available(monkeypatch, [reservation]) == {}

    def test_skips_other_instance_types(self, monkeypatch):
        reservation = {**self.RESERVATION, 'instance_type': 'it_h100'}
        assert self._available(monkeypatch, [reservation]) == {}

    def test_matches_instance_type_by_name_across_fids(self, monkeypatch):
        # The name maps to a different FID per region; either must match.
        reservation = {**self.RESERVATION, 'instance_type': 'it_a100_other'}
        assert self._available(monkeypatch, [reservation]) == {
            'umang-jordan': 4
        }

    def test_specific_reservations_filters_by_name_or_fid(self, monkeypatch):
        assert self._available(monkeypatch, [self.RESERVATION],
                               specific_reservations={'umang-jordan'}) == {
                                   'umang-jordan': 4
                               }
        assert self._available(monkeypatch, [self.RESERVATION],
                               specific_reservations={'res_abc'}) == {
                                   'umang-jordan': 4
                               }
        assert self._available(monkeypatch, [self.RESERVATION],
                               specific_reservations={'other'}) == {}

    def test_no_reservations(self, monkeypatch):
        assert self._available(monkeypatch, []) == {}

    def test_cloud_hook_returns_capacity(self, monkeypatch):
        self._patch(monkeypatch, [self.RESERVATION])
        result = mithril.Mithril().get_reservations_available_resources(
            instance_type='a100-80gb.sxm.4x',
            region='us-central3-a',
            zone=None,
            specific_reservations=set())
        assert result == {'umang-jordan': 4}

    def test_cloud_hook_degrades_to_empty_on_error(self, monkeypatch):
        # A reservation lookup failure must fall back to bidding, not raise.
        def boom(**kwargs):
            raise mithril_utils.MithrilError('API down')

        monkeypatch.setattr(mithril_utils, 'get_available_reservations', boom)
        result = mithril.Mithril().get_reservations_available_resources(
            instance_type='a100-80gb.sxm.4x',
            region='us-central3-a',
            zone=None,
            specific_reservations=set())
        assert result == {}


class TestMithrilReservationSelection:
    """Selecting the reservation a cluster will run on."""

    RESERVATION = {
        'fid': 'res_abc',
        'name': 'umang-jordan',
        'status': 'Active',
        'region': 'us-central3-a',
        'instance_type': 'it_a100',
        'instance_quantity': 4,
        'end_time': '2099-01-01T00:00:00Z',
    }

    def _patch(self, monkeypatch, reservations):
        monkeypatch.setattr(mithril_utils,
                            'list_reservations',
                            lambda config=None: reservations)
        monkeypatch.setattr(
            mithril_utils, 'get_instance_types',
            lambda: {'it_a100': {
                'fid': 'it_a100',
                'name': 'a100-80gb.sxm.4x'
            }})

    def test_picks_reservation_large_enough(self, monkeypatch):
        self._patch(monkeypatch, [self.RESERVATION])
        found = mithril_utils.find_reservation_for_cluster('a100-80gb.sxm.4x',
                                                           'us-central3-a',
                                                           num_nodes=4)
        assert found is not None and found['fid'] == 'res_abc'

    def test_skips_reservation_smaller_than_cluster(self, monkeypatch):
        # A reservation is consumed whole, so a smaller one is not a partial
        # match; growing it is impossible.
        self._patch(monkeypatch, [{**self.RESERVATION, 'instance_quantity': 2}])
        assert mithril_utils.find_reservation_for_cluster(
            'a100-80gb.sxm.4x', 'us-central3-a', num_nodes=4) is None

    def test_prefers_the_reservation_expiring_soonest(self, monkeypatch):
        # Both fit. Consuming the one about to lapse uses capacity that would
        # otherwise be lost, and keeps the longer-lived one for later work.
        self._patch(monkeypatch, [
            {
                **self.RESERVATION, 'name': 'aaa-later',
                'fid': 'res_late',
                'end_time': '2099-06-01T00:00:00Z'
            },
            {
                **self.RESERVATION, 'name': 'zzz-sooner',
                'fid': 'res_soon',
                'end_time': '2099-01-02T00:00:00Z'
            },
        ])
        found = mithril_utils.find_reservation_for_cluster('a100-80gb.sxm.4x',
                                                           'us-central3-a',
                                                           num_nodes=1)
        assert found is not None
        assert found['name'] == 'zzz-sooner', (
            'expiry must beat alphabetical order')

    def test_equal_expiry_falls_back_to_name(self, monkeypatch):
        self._patch(monkeypatch, [
            {
                **self.RESERVATION, 'name': 'zzz',
                'fid': 'res_z'
            },
            {
                **self.RESERVATION, 'name': 'aaa',
                'fid': 'res_a'
            },
        ])
        found = mithril_utils.find_reservation_for_cluster('a100-80gb.sxm.4x',
                                                           'us-central3-a',
                                                           num_nodes=1)
        assert found is not None and found['name'] == 'aaa'

    def test_soonest_expiry_still_respects_size(self, monkeypatch):
        # The soonest to expire is too small, so the later one wins.
        self._patch(monkeypatch, [
            {
                **self.RESERVATION, 'name': 'small-soon',
                'fid': 'res_small',
                'instance_quantity': 1,
                'end_time': '2099-01-02T00:00:00Z'
            },
            {
                **self.RESERVATION, 'name': 'big-later',
                'fid': 'res_big',
                'instance_quantity': 8,
                'end_time': '2099-06-01T00:00:00Z'
            },
        ])
        found = mithril_utils.find_reservation_for_cluster('a100-80gb.sxm.4x',
                                                           'us-central3-a',
                                                           num_nodes=4)
        assert found is not None and found['name'] == 'big-later'

    def test_reservation_is_finished(self):
        assert not mithril_utils.reservation_is_finished(self.RESERVATION)
        assert mithril_utils.reservation_is_finished({
            **self.RESERVATION, 'status': 'Ended'
        })
        assert mithril_utils.reservation_is_finished({
            **self.RESERVATION, 'end_time': '2020-01-01T00:00:00Z'
        })
        # Paused is usable, not finished: it resumes with a single PATCH.
        assert not mithril_utils.reservation_is_finished({
            **self.RESERVATION, 'status': 'Paused'
        })

    def test_get_reservation_by_fid(self, monkeypatch):
        self._patch(monkeypatch, [self.RESERVATION])
        assert mithril_utils.get_reservation('res_abc')['name'] == (
            'umang-jordan')
        assert mithril_utils.get_reservation('res_missing') is None


class TestMithrilReservationBackedProvisioning:
    """Provider behaviour once a cluster is bound to a reservation."""

    def test_reservation_fid_read_from_provider_config(self):
        assert mithril_instance._reservation_fid(None) is None
        assert mithril_instance._reservation_fid({}) is None
        # Bid-backed clusters persist an empty string, not a missing key.
        assert mithril_instance._reservation_fid({'reservation': ''}) is None
        assert mithril_instance._reservation_fid({'reservation': 'res_abc'
                                                 }) == 'res_abc'

    def _instances(self):
        return {
            'inst_reserved': {
                'name': 'umang-jordan-1',
                'status': 'STATUS_RUNNING',
                'reservation': 'res_abc',
            },
            'inst_bid': {
                'name': 'sky-cluster-abc-1',
                'status': 'STATUS_RUNNING',
                'reservation': None,
            },
        }

    def test_filters_by_reservation_not_name(self, monkeypatch):
        # The cluster name never prefixes a reserved instance's name, so the
        # reservation FID is the only usable handle.
        monkeypatch.setattr(mithril_utils,
                            'list_instances',
                            lambda config=None: self._instances())
        found = mithril_instance._filter_instances('sky-cluster-abc',
                                                   reservation_fid='res_abc')
        assert list(found) == ['inst_reserved']

    def test_bid_path_still_filters_by_name(self, monkeypatch):
        monkeypatch.setattr(mithril_utils,
                            'list_instances',
                            lambda config=None: self._instances())
        found = mithril_instance._filter_instances('sky-cluster-abc')
        assert list(found) == ['inst_bid']

    def test_resume_refuses_finished_reservation(self, monkeypatch):
        # Pausing/resuming past the window is irreversible server-side.
        monkeypatch.setattr(mithril_utils,
                            'get_reservation',
                            lambda fid, config=None: {
                                'fid': fid,
                                'name': 'umang-jordan',
                                'status': 'Ended',
                                'end_time': '2020-01-01T00:00:00Z',
                            })
        with pytest.raises(mithril_utils.MithrilError, match='has ended'):
            mithril_instance._resume_reservation_if_paused(
                'res_abc', resume_stopped_nodes=True)

    def test_resume_paused_reservation(self, monkeypatch):
        calls = []
        monkeypatch.setattr(mithril_utils,
                            'get_reservation',
                            lambda fid, config=None: {
                                'fid': fid,
                                'name': 'umang-jordan',
                                'status': 'Paused',
                                'end_time': '2099-01-01T00:00:00Z',
                                'instances': ['inst_1'],
                            })
        monkeypatch.setattr(mithril_utils,
                            'update_reservation',
                            lambda fid, paused, config=None: calls.append(
                                (fid, paused)))
        resumed = mithril_instance._resume_reservation_if_paused(
            'res_abc', resume_stopped_nodes=True)
        assert resumed == ['inst_1']
        assert calls == [('res_abc', False)]

    def test_terminate_pauses_instead_of_cancelling(self, monkeypatch):
        # ml down must never cancel or return reserved capacity: it is paid for
        # up front and cannot be un-bought.
        calls = []
        monkeypatch.setattr(mithril_utils,
                            'get_reservation',
                            lambda fid, config=None: {
                                'fid': fid,
                                'name': 'umang-jordan',
                                'status': 'Active',
                                'end_time': '2099-01-01T00:00:00Z',
                            })
        monkeypatch.setattr(mithril_utils,
                            'update_reservation',
                            lambda fid, paused, config=None: calls.append(
                                (fid, paused)))

        def fail(*args, **kwargs):
            raise AssertionError('bid APIs must not be touched')

        monkeypatch.setattr(mithril_utils, 'cancel_bid', fail)
        monkeypatch.setattr(mithril_utils, 'get_bid', fail)

        mithril_instance.terminate_instances(
            'sky-cluster-abc', provider_config={'reservation': 'res_abc'})
        assert calls == [('res_abc', True)]

    def test_terminate_is_idempotent_on_paused(self, monkeypatch):
        monkeypatch.setattr(mithril_utils,
                            'get_reservation',
                            lambda fid, config=None: {
                                'fid': fid,
                                'name': 'umang-jordan',
                                'status': 'Paused',
                                'end_time': '2099-01-01T00:00:00Z',
                            })

        def fail(*args, **kwargs):
            raise AssertionError('should not PATCH an already-paused '
                                 'reservation')

        monkeypatch.setattr(mithril_utils, 'update_reservation', fail)
        mithril_instance.terminate_instances(
            'sky-cluster-abc', provider_config={'reservation': 'res_abc'})

    def test_terminate_leaves_expired_reservation_alone(self, monkeypatch):
        monkeypatch.setattr(mithril_utils,
                            'get_reservation',
                            lambda fid, config=None: {
                                'fid': fid,
                                'name': 'umang-jordan',
                                'status': 'Active',
                                'end_time': '2020-01-01T00:00:00Z',
                            })

        def fail(*args, **kwargs):
            raise AssertionError('pausing an expired reservation is '
                                 'irreversible')

        monkeypatch.setattr(mithril_utils, 'update_reservation', fail)
        mithril_instance.terminate_instances(
            'sky-cluster-abc', provider_config={'reservation': 'res_abc'})

    def test_stop_pauses_reservation(self, monkeypatch):
        calls = []
        monkeypatch.setattr(mithril_utils,
                            'get_reservation',
                            lambda fid, config=None: {
                                'fid': fid,
                                'name': 'umang-jordan',
                                'status': 'Active',
                                'end_time': '2099-01-01T00:00:00Z',
                            })
        monkeypatch.setattr(mithril_utils,
                            'update_reservation',
                            lambda fid, paused, config=None: calls.append(
                                (fid, paused)))
        monkeypatch.setattr(mithril_utils, 'update_bid',
                            lambda *a, **k: pytest.fail('bid must not change'))
        mithril_instance.stop_instances(
            'sky-cluster-abc', provider_config={'reservation': 'res_abc'})
        assert calls == [('res_abc', True)]


class TestMithrilReservationDiagnostics:
    """Explaining why a requested reservation cannot be used."""

    BASE = {
        'fid': 'res_1',
        'name': 'candidate',
        'status': 'Active',
        'region': 'us-central3-a',
        'instance_type': 'it_a100',
        'instance_quantity': 4,
        'end_time': '2099-01-01T00:00:00Z',
    }

    def _patch(self, monkeypatch, reservations):
        monkeypatch.setattr(mithril_utils,
                            'list_reservations',
                            lambda config=None: reservations)
        monkeypatch.setattr(
            mithril_utils, 'get_instance_types', lambda: {
                'it_a100': {
                    'name': 'a100-80gb.sxm.4x'
                },
                'it_h100': {
                    'name': 'h100-80gb.sxm.8x'
                },
            })

    def _explain(self, monkeypatch, reservations, num_nodes=4):
        self._patch(monkeypatch, reservations)
        return mithril_utils.explain_unusable_reservations(
            'a100-80gb.sxm.4x', 'us-central3-a', num_nodes)

    def test_no_reservations_at_all(self, monkeypatch):
        assert self._explain(monkeypatch,
                             []) == ['No reservations exist in this project.']

    def test_reports_terminal_status(self, monkeypatch):
        reasons = self._explain(monkeypatch, [{
            **self.BASE, 'status': 'Canceled'
        }])
        assert reasons == ['candidate: status is Canceled.']

    def test_reports_closed_window(self, monkeypatch):
        reasons = self._explain(monkeypatch, [{
            **self.BASE, 'end_time': '2020-01-01T00:00:00Z'
        }])
        assert 'window closed at 2020-01-01T00:00:00Z' in reasons[0]

    def test_reports_wrong_region(self, monkeypatch):
        reasons = self._explain(monkeypatch, [{
            **self.BASE, 'region': 'us-east1-a'
        }])
        assert reasons == ['candidate: is in us-east1-a, not us-central3-a.']

    def test_reports_wrong_instance_type_by_name(self, monkeypatch):
        reasons = self._explain(monkeypatch, [{
            **self.BASE, 'instance_type': 'it_h100'
        }])
        assert reasons == [
            'candidate: holds h100-80gb.sxm.8x, not a100-80gb.sxm.4x.'
        ]

    def test_reports_too_small(self, monkeypatch):
        reasons = self._explain(monkeypatch, [{
            **self.BASE, 'instance_quantity': 2
        }])
        assert 'holds 2 instance(s), fewer than the 4' in reasons[0]
        assert 'cannot grow' in reasons[0]

    def test_named_reservation_that_does_not_exist(self, monkeypatch):
        self._patch(monkeypatch, [self.BASE])
        reasons = mithril_utils.explain_unusable_reservations(
            'a100-80gb.sxm.4x',
            'us-central3-a',
            1,
            specific_reservations={'nope'})
        assert reasons == ['No reservation named nope exists in this project.']


class TestMithrilPauseSettles:
    """Waiting for a pause to finish before returning to SkyPilot."""

    RES = {
        'fid': 'res_abc',
        'name': 'umang-jordan',
        'status': 'Active',
        'end_time': '2099-01-01T00:00:00Z',
    }

    def _instances(self, status):
        return {
            'inst_a': {
                'name': 'umang-jordan-1',
                'status': status,
                'reservation': 'res_abc',
            }
        }

    def test_waits_until_instances_report_paused(self, monkeypatch):
        # SkyPilot rejects STATUS_STOPPING (it maps to UP) right after teardown,
        # so terminate must not return until the pause has settled.
        seen = []
        states = ['STATUS_STOPPING', 'STATUS_STOPPING', 'STATUS_PAUSED']

        def fake_list_instances(config=None):
            state = states[min(len(seen), len(states) - 1)]
            seen.append(state)
            return self._instances(state)

        monkeypatch.setattr(mithril_utils, 'list_instances',
                            fake_list_instances)
        monkeypatch.setattr(mithril_instance.time, 'sleep', lambda _: None)
        mithril_instance._wait_for_reservation_paused('res_abc')
        assert seen[-1] == 'STATUS_PAUSED'
        assert len(seen) >= 3, 'should have polled while still stopping'

    def test_gives_up_quietly_when_the_pause_never_settles(self, monkeypatch):
        # Failing the teardown would be worse than reporting what is there.
        monkeypatch.setattr(
            mithril_utils,
            'list_instances',
            lambda config=None: self._instances('STATUS_STOPPING'))
        monkeypatch.setattr(mithril_instance.time, 'sleep', lambda _: None)
        clock = iter([0.0] + [10_000.0] * 20)
        monkeypatch.setattr(mithril_instance.time, 'time', lambda: next(clock))
        mithril_instance._wait_for_reservation_paused(
            'res_abc')  # must not raise

    def test_terminate_waits_before_returning(self, monkeypatch):
        calls = []
        monkeypatch.setattr(mithril_utils,
                            'get_reservation',
                            lambda fid, config=None: dict(self.RES))
        monkeypatch.setattr(mithril_utils,
                            'update_reservation',
                            lambda fid, paused, config=None: calls.append(
                                ('patch', paused)))
        monkeypatch.setattr(mithril_instance,
                            '_wait_for_reservation_paused',
                            lambda fid, config=None: calls.append(('waited',)))
        mithril_instance.terminate_instances(
            'sky-cluster', provider_config={'reservation': 'res_abc'})
        assert calls == [('patch', True), ('waited',)], calls
