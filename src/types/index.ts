export interface TranscodeJobData {
    videoId: string;
    userId: string;
    userType: 'user' | 'creator' | 'ad';
    r2Key: string;
    qualities: string[];
    callbackUrl?: string;
}
