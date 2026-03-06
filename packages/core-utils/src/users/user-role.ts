import { UserRight, UserRightType, UserRole, UserRoleType } from '@peertube/peertube-models'

export const USER_RIGHT_LABELS: { [ id in UserRightType ]: string } = {
  [UserRight.ALL]: 'Full access',
  [UserRight.MANAGE_USERS]: 'Manage users',
  [UserRight.MANAGE_SERVER_FOLLOW]: 'Manage server follow',
  [UserRight.MANAGE_LOGS]: 'Manage logs',
  [UserRight.MANAGE_DEBUG]: 'Manage debug',
  [UserRight.MANAGE_SERVER_REDUNDANCY]: 'Manage server redundancy',
  [UserRight.MANAGE_ABUSES]: 'Manage abuses',
  [UserRight.MANAGE_JOBS]: 'Manage jobs',
  [UserRight.MANAGE_CONFIGURATION]: 'Manage configuration',
  [UserRight.MANAGE_INSTANCE_CUSTOM_PAGE]: 'Manage instance custom page',
  [UserRight.MANAGE_ACCOUNTS_BLOCKLIST]: 'Manage accounts blocklist',
  [UserRight.MANAGE_SERVERS_BLOCKLIST]: 'Manage servers blocklist',
  [UserRight.MANAGE_VIDEO_BLACKLIST]: 'Manage video blacklist',
  [UserRight.MANAGE_ANY_VIDEO_CHANNEL]: 'Manage any video channel',
  [UserRight.REMOVE_ANY_VIDEO]: 'Remove any video',
  [UserRight.REMOVE_ANY_VIDEO_PLAYLIST]: 'Remove any video playlist',
  [UserRight.MANAGE_ANY_VIDEO_COMMENT]: 'Manage any video comment',
  [UserRight.UPDATE_ANY_VIDEO]: 'Update any video',
  [UserRight.UPDATE_ANY_VIDEO_PLAYLIST]: 'Update any video playlist',
  [UserRight.GET_ANY_LIVE]: 'Get any live',
  [UserRight.SEE_ALL_VIDEOS]: 'See all videos',
  [UserRight.SEE_ALL_COMMENTS]: 'See all comments',
  [UserRight.CHANGE_VIDEO_OWNERSHIP]: 'Change video ownership',
  [UserRight.MANAGE_PLUGINS]: 'Manage plugins',
  [UserRight.MANAGE_VIDEOS_REDUNDANCIES]: 'Manage videos redundancies',
  [UserRight.MANAGE_VIDEO_FILES]: 'Manage video files',
  [UserRight.RUN_VIDEO_TRANSCODING]: 'Run video transcoding',
  [UserRight.MANAGE_VIDEO_IMPORTS]: 'Manage video imports',
  [UserRight.MANAGE_REGISTRATIONS]: 'Manage registrations',
  [UserRight.MANAGE_RUNNERS]: 'Manage runners',
  [UserRight.MANAGE_USER_EXPORTS]: 'Manage user exports',
  [UserRight.MANAGE_USER_IMPORTS]: 'Manage user imports',
  [UserRight.MANAGE_INSTANCE_WATCHED_WORDS]: 'Manage instance watched words',
  [UserRight.MANAGE_INSTANCE_AUTO_TAGS]: 'Manage instance auto tags'
}

export const USER_ROLE_LABELS: { [ id in UserRoleType ]: string } = {
  [UserRole.USER]: 'User',
  [UserRole.MODERATOR]: 'Moderator',
  [UserRole.ADMINISTRATOR]: 'Administrator'
}

const userRoleRights: { [ id in UserRoleType ]: UserRightType[] } = {
  [UserRole.ADMINISTRATOR]: [
    UserRight.ALL
  ],

  [UserRole.MODERATOR]: [
    UserRight.MANAGE_VIDEO_BLACKLIST,
    UserRight.MANAGE_ABUSES,
    UserRight.MANAGE_ANY_VIDEO_CHANNEL,
    UserRight.REMOVE_ANY_VIDEO,
    UserRight.REMOVE_ANY_VIDEO_PLAYLIST,
    UserRight.MANAGE_ANY_VIDEO_COMMENT,
    UserRight.UPDATE_ANY_VIDEO,
    UserRight.SEE_ALL_VIDEOS,
    UserRight.MANAGE_ACCOUNTS_BLOCKLIST,
    UserRight.MANAGE_SERVERS_BLOCKLIST,
    UserRight.MANAGE_USERS,
    UserRight.SEE_ALL_COMMENTS,
    UserRight.MANAGE_REGISTRATIONS,
    UserRight.MANAGE_INSTANCE_WATCHED_WORDS,
    UserRight.MANAGE_INSTANCE_AUTO_TAGS
  ],

  [UserRole.USER]: []
}

export function hasUserRight (userRole: UserRoleType, userRight: UserRightType) {
  const userRights = userRoleRights[userRole]

  return userRights.includes(UserRight.ALL) || userRights.includes(userRight)
}
