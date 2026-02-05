export interface TranscodeJobData {
    videoId: string;
    userId: string;
    userType: 'user' | 'creator';
    r2Key: string;
    qualities: string[];
}
